"""
This module defines a job queue and a worker pool implementation to process
jobs submitted by dbk applications. The worker pool is a set of child processes.
"""

import asyncio
import logging
import multiprocessing as mp
import threading
import queue
import time
from datetime import datetime
from types import TracebackType
from typing import ContextManager, Literal, Self

from dbk import errors

log = logging.getLogger(__name__)

JobStatus = Literal["pending", "running", "completed", "failed", "cancelled"]


class Job[TOut]:
    def __init__(self, fn, *args, **kwargs):
        self.id = -1
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.result: TOut | None = None
        self.error: Exception | None = None
        self.called = False
        self.submitted_at: datetime | None = None
        self.cancelled_at: datetime | None = None
        self.started_at: datetime | None = None
        self.finished_at: datetime | None = None

    def eval(self) -> TOut | None:
        if self.called:
            return self.result
        self.started_at = datetime.now()

        try:
            self.result = self.fn(*self.args, **self.kwargs)
        except Exception as e:
            self.error = e

        self.finished_at = datetime.now()
        self.called = True

        return self.result

    @property
    def name(self):
        return self.fn.__name__

    @property
    def status(self) -> JobStatus:
        if self.called:
            return "completed" if not self.error else "failed"
        if not self.submitted_at:
            return "pending"
        if self.cancelled_at:
            return "cancelled"
        return "running"

    def __repr__(self):
        return f"Job(id={self.id}, name={self.name}, status={self.status})"


class JobHandle[TOut]:
    def __init__(self, job: Job[TOut], task: asyncio.Future[TOut | None]):
        self._job = job
        self._task = task

    @property
    def id(self):
        return self._job.id

    @property
    def status(self):
        return self._job.status

    @property
    def job(self):
        return self._job

    @job.setter
    def job(self, value: Job) -> None:
        assert value.id == self.id
        assert value.status in ("completed", "failed", "cancelled")
        self._job = value

        if value.error:
            log.warning("job %s failed: %s", value, value.error)
            self._task.set_exception(value.error)
        else:
            log.debug("job %s completed", value)
            self._task.set_result(value.result)

    def __repr__(self) -> str:
        return repr(self._job)

    def __await__(self):
        return self._task.__await__()


class _Counter:
    def __init__(self):
        self._value = 0

    def next(self):
        self._value += 1
        return self._value


class JobCancelledError(errors.DbkError):
    def __init__(self, handle: JobHandle, *args):
        assert handle.status == "cancelled"
        self.handle = handle
        super().__init__(*args)


def _worker(inputs: mp.Queue, outputs: mp.Queue):
    while True:
        job: Job | None = inputs.get()
        if job is None:
            break
        job.eval()
        outputs.put(job)


def _process_results(
    results: mp.Queue,
    pending: dict[int, JobHandle],
    stop: threading.Event,
):
    while True:
        if stop.is_set():
            break

        try:
            job: Job | None = results.get_nowait()

            if job is None:
                continue

            log.debug("received result for job %s", job)

            if (handle := pending.pop(job.id)) is None:
                log.warning("received result for unknown job %s", job)
                continue

            handle.job = job
        except queue.Empty:
            time.sleep(0.1)


class WorkerPool(ContextManager[Self]):
    """
    A pool of worker processes to handle cpu-intensive tasks that should not
    block the UI thread.
    """

    def __init__(
        self,
        n_workers: int = 0,
    ):
        self._job_queue = mp.Queue()
        """Jobs to submit to the pool."""

        self._results_queue = mp.Queue()
        """Results from the pool."""

        self._processes: list[mp.Process] = []
        """Child processes."""

        self._pending_jobs: dict[int, JobHandle] = {}
        """Keeps track of which jobs are submitted."""

        if n_workers <= 0:
            n_workers = mp.cpu_count()

        self._next_job_id = _Counter()
        self._n_workers = n_workers

        self._started = False
        """Keeps track of whether the start() method has been called."""

        self._stop = threading.Event()
        """Used to signal threads to stop."""

        self._results_processor_thread = threading.Thread(
            target=_process_results,
            args=(
                self._results_queue,
                self._pending_jobs,
                self._stop,
            ),
        )
        """Reads completed jobs from the results queue and updates the job handles."""

    def submit[
        T
    ](self, job: Job[T],) -> JobHandle[T]:
        task = asyncio.Future()
        job.id = self._next_job_id.next()
        job.submitted_at = datetime.now()
        handle = JobHandle(job, task)
        self._pending_jobs[job.id] = handle
        self._job_queue.put_nowait(job)
        log.debug("submitted job %s", job)
        return handle

    def start(self) -> None:
        if self._started:
            return

        for _ in range(self._n_workers):
            p = mp.Process(
                target=_worker,
                args=(self._job_queue, self._results_queue),
                daemon=False,
            )
            p.start()
            log.debug("worker process %s started", p.pid)
            self._processes.append(p)

        self._started = True
        log.info(f"initialized {self._n_workers} background workers")

        self._results_processor_thread.start()

    def stop(self, timeout: float = 2.0):
        if not self._started:
            return

        # send poison pills
        for _ in range(self._n_workers):
            self._job_queue.put(None)

        # wait for workers to terminate
        for p in self._processes:
            try:
                p.join(timeout)
                log.debug("worker process %s terminated", p.pid)
            except TimeoutError:
                log.warning(
                    "worker process %s did not terminate within %s seconds",
                    p.pid,
                    timeout,
                )
                p.kill()

        if self._results_processor_thread.is_alive():
            try:
                log.debug("stopping results processing thread...")
                self._stop.set()
                self._results_processor_thread.join()
                log.debug("results processing thread stopped")
            except Exception as e:
                log.exception("failed to stop results processing thread")
            finally:
                self._stop.clear()

        self._processes.clear()
        self._started = False

        for handle in self._pending_jobs.values():
            if not handle._task.done():
                log.warning("job %s never completed, it will be cancelled", handle)
                handle.job.cancelled_at = datetime.now()
                handle._task.set_exception(JobCancelledError(handle))

        self._pending_jobs.clear()

        log.info("worker pool terminated")

    def __enter__(self) -> Self:
        self.start()
        return super().__enter__()

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None:
        self.stop()
        return super().__exit__(__exc_type, __exc_value, __traceback)
