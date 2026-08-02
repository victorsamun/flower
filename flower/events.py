import asyncio
import collections
import datetime
import logging
import shelve
import threading
from collections import Counter
from functools import partial
from concurrent.futures import ThreadPoolExecutor

from celery.events import EventReceiver
from celery.events.state import State
from prometheus_client import Counter as PrometheusCounter
from prometheus_client import Gauge, Histogram
from prometheus_client.metrics import MetricWrapperBase
from tornado.ioloop import PeriodicCallback
from tornado.options import options

from .utils.broker import Broker

logger = logging.getLogger(__name__)

PROMETHEUS_METRICS = None


def get_prometheus_metrics():
    global PROMETHEUS_METRICS  # pylint: disable=global-statement
    if PROMETHEUS_METRICS is None:
        PROMETHEUS_METRICS = PrometheusMetrics()

    return PROMETHEUS_METRICS


class PrometheusMetrics:
    def __init__(self):
        self.app = None
        self.events = PrometheusCounter('flower_events_total', "Number of events", ['worker', 'type', 'task'])

        self.runtime = Histogram(
            'flower_task_runtime_seconds',
            "Task runtime",
            ['worker', 'task'],
            buckets=options.task_runtime_metric_buckets
        )
        self.prefetch_time = Gauge(
            'flower_task_prefetch_time_seconds',
            "The time the task spent waiting at the celery worker to be executed.",
            ['worker', 'task']
        )
        self.queued_time = Histogram(
            'flower_task_queued_time',
            'The time the task spent in queue',
            ['worker', 'task']
        )
        self.number_of_prefetched_tasks = Gauge(
            'flower_worker_prefetched_tasks',
            'Number of tasks of given type prefetched at a worker',
            ['worker', 'task']
        )
        self.worker_online = Gauge('flower_worker_online', "Worker online status", ['worker'])
        self.worker_number_of_currently_executing_tasks = Gauge(
            'flower_worker_number_of_currently_executing_tasks',
            "Number of tasks currently executing at a worker",
            ['worker']
        )
        self.queue_length = Gauge(
            'flower_queue_length',
            "Number of messages in a broker queue",
            ['queue']
        )
        self._configure_queue_length_metric()

    def configure_queue_metrics(self, app):
        self.app = app

    def _configure_queue_length_metric(self):
        # Gauge.set_function only works on labeled children, so refresh
        # callbacks for active queues right before samples are collected.
        def multi_samples():
            self.queue_length.clear()
            lengths = self._fetch_queue_lengths()
            if lengths is not None:
                for queue_name in self._get_active_queue_names():
                    self.queue_length.labels(queue_name).set_function(
                        lambda q=queue_name: float(lengths.get(q, 0))
                    )
            return MetricWrapperBase._multi_samples(self.queue_length)

        self.queue_length._multi_samples = multi_samples

    def _get_active_queue_names(self):
        if self.app is None:
            return []

        queues = set()
        for _, info in self.app.workers.items():
            for queue in info.get('active_queues', []):
                queues.add(queue['name'])

        if not queues:
            capp = self.app.capp
            queues = {capp.conf.task_default_queue} | {
                q.name for q in capp.conf.task_queues or [] if q.name
            }
        return sorted(queues)

    def _fetch_queue_lengths(self):
        if self.app is None:
            return None

        try:
            app = self.app
            http_api = None
            if app.transport == 'amqp' and app.options.broker_api:
                http_api = app.options.broker_api

            with app.capp.connection() as conn:
                broker = Broker(
                    conn.as_uri(include_password=True),
                    http_api=http_api,
                    broker_options=app.capp.conf.broker_transport_options,
                    broker_use_ssl=app.capp.conf.broker_use_ssl,
                )
                queues = self._run_async(broker.queues(self._get_active_queue_names()))

            return {queue['name']: queue['messages'] for queue in queues or []}
        except Exception as e:
            logger.warning("Unable to get queue lengths: '%s'", e)
            return None

    @staticmethod
    def _run_async(coro):
        """Run a coroutine from sync Gauge callbacks (IOLoop may already be running)."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)

        # Avoid deadlock on the running loop: finish the coroutine in a new loop.
        with ThreadPoolExecutor(max_workers=1) as executor:
            return executor.submit(asyncio.run, coro).result()


class EventsState(State):
    # EventsState object is created and accessed only from ioloop thread

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.counter = collections.defaultdict(Counter)
        self.metrics = get_prometheus_metrics()

    def event(self, event):
        # Save the event
        super().event(event)

        worker_name = event['hostname']
        event_type = event['type']

        self.counter[worker_name][event_type] += 1

        if event_type.startswith('task-'):
            task_id = event['uuid']
            task = self.tasks.get(task_id)
            task_name = event.get('name', '')
            if not task_name and task_id in self.tasks:
                task_name = task.name or ''
            self.metrics.events.labels(worker_name, event_type, task_name).inc()

            task_eta = task.eta
            task_sent = task.sent
            task_started = task.started
            task_received = task.received

            runtime = event.get('runtime')
            if runtime is None and task_started:
                if event_type == 'task-failed' and task.failed:
                    runtime = task.failed - task_started
                elif event_type == 'task-retried' and task.retried:
                    runtime = task.retried - task_started
            if runtime is not None:
                self.metrics.runtime.labels(worker_name, task_name).observe(runtime)

            if event_type == 'task-received' and not task_eta and task_received:
                self.metrics.number_of_prefetched_tasks.labels(worker_name, task_name).inc()

            if event_type == 'task-started' and task_started and task_received:
                if not task_eta:
                    self.metrics.prefetch_time.labels(worker_name, task_name).set(task_started - task_received)
                    self.metrics.number_of_prefetched_tasks.labels(worker_name, task_name).dec()
                    queued_time = task_started - task_sent if task_sent else None
                else:
                    queued_time = task_started - datetime.datetime.fromisoformat(task_eta).timestamp()

                if queued_time:
                    self.metrics.queued_time.labels(worker_name, task_name).observe(queued_time)

            if event_type in ['task-succeeded', 'task-failed'] and not task.eta and task_started and task_received:
                self.metrics.prefetch_time.labels(worker_name, task_name).set(0)

        if event_type == 'worker-online':
            self.metrics.worker_online.labels(worker_name).set(1)

        if event_type == 'worker-heartbeat':
            self.metrics.worker_online.labels(worker_name).set(1)

            num_executing_tasks = event.get('active')
            if num_executing_tasks is not None:
                self.metrics.worker_number_of_currently_executing_tasks.labels(worker_name).set(num_executing_tasks)

        if event_type == 'worker-offline':
            self.metrics.worker_online.labels(worker_name).set(0)


class Events(threading.Thread):
    events_enable_interval = 5000

    # pylint: disable=too-many-arguments
    def __init__(self, capp, io_loop, db=None, persistent=False,
                 enable_events=True, state_save_interval=0,
                 **kwargs):
        threading.Thread.__init__(self)
        self.daemon = True

        self.io_loop = io_loop
        self.capp = capp

        self.db = db
        self.persistent = persistent
        self.enable_events = enable_events
        self.state = None
        self.state_save_timer = None

        if self.persistent:
            logger.debug("Loading state from '%s'...", self.db)
            state = shelve.open(self.db)
            if state:
                self.state = state['events']
            state.close()

            if state_save_interval:
                self.state_save_timer = PeriodicCallback(self.save_state,
                                                         state_save_interval)

        if not self.state:
            self.state = EventsState(**kwargs)

        self.timer = PeriodicCallback(self.on_enable_events,
                                      self.events_enable_interval)

    def configure_queue_metrics(self, app):
        get_prometheus_metrics().configure_queue_metrics(app)

    def start(self):
        threading.Thread.start(self)
        if self.enable_events:
            logger.debug("Starting enable events timer...")
            self.timer.start()

        if self.state_save_timer:
            logger.debug("Starting state save timer...")
            self.state_save_timer.start()

    def stop(self):
        if self.enable_events:
            logger.debug("Stopping enable events timer...")
            self.timer.stop()

        if self.state_save_timer:
            logger.debug("Stopping state save timer...")
            self.state_save_timer.stop()

        if self.persistent:
            self.save_state()

    def run(self):
        conf = self.capp.conf

        def on_connection_error(exc, interval):
            logger.error("Failed to capture events: '%s', "
                         "trying again in %s seconds.",
                         exc, interval)
            logger.debug(exc, exc_info=True)

        while True:
            try:
                with self.capp.connection_for_read() as conn:
                    if conf.broker_connection_retry:
                        conn.ensure_connection(
                            on_connection_error,
                            conf.broker_connection_max_retries,
                        )
                    else:
                        conn.connect()

                    recv = EventReceiver(conn,
                                         handlers={"*": self.on_event},
                                         app=self.capp)
                    logger.debug("Capturing events...")
                    try:
                        recv.capture(limit=None, timeout=None, wakeup=True)
                    except conn.connection_errors + conn.channel_errors as exc:
                        logger.error("Failed to capture events: '%s', "
                                     "trying to reconnect...",
                                     exc)
                        logger.debug(exc, exc_info=True)
                        if not conf.broker_connection_retry:
                            raise
            except (KeyboardInterrupt, SystemExit):
                try:
                    import _thread as thread
                except ImportError:
                    import thread
                thread.interrupt_main()
                break

    def save_state(self):
        logger.debug("Saving state to '%s'...", self.db)
        state = shelve.open(self.db, flag='n')
        state['events'] = self.state
        state.close()

    def on_enable_events(self):
        # Periodically enable events for workers
        # launched after flower
        self.io_loop.run_in_executor(None, self.capp.control.enable_events)

    def on_event(self, event):
        # Call EventsState.event in ioloop thread to avoid synchronization
        self.io_loop.add_callback(partial(self.state.event, event))
