# Copyright 2023 - Greg Hecht - All Rights Reserved
# Licensed to Sola Insurance, all modifications and reuse permitted within that organization.


import logging
import multiprocessing
import os
import threading

from logging.handlers import QueueHandler
from typing import Any, MutableMapping, Union

"""Utility for logging in a multiprocessing simulation.

Logging with multiprocessing can be challenge. It comes with several issues:
* Logger(s) from the main process are not available in the sub-processes. So if you want to see logs from the sub-proc
  you need to independently initialize logging in them.

* It's confusing what should and shouldn't work. Logging in a multi-threaded environment requires no extra effort. 
  And, logging in a multi-process environment *may* work fine, but depends on whether your outputting to a single file. 
  If so, you may have collisions. 

* The current [recipe](https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes)
  for handling multi-process logging is to use a QueueHandler. On the input end, processes write their log messages into
  the handler's queue. On the output end, a thread or process reads from the queue and logs the message, effectively
  funneling all parallel logging into a single pipeline. But...
  
* The recipe doesn't work AFAICT. It has each process add a QueueHandler. When I tried it, it resulted in replicated
  log-lines. The solution is to have a single sub-process add the QueueHandler meaning all other sub-processes need
  to check for it: a potential race condition. 

* When each process logs, there's no automatic distinction between the processes. One could modify the logging format
  to include the pid. But what if we're conditionally not running in a sub-process? Then it's just wasted space. So 
  we needed a way to conditionally add in the sub-process, and while we're at it, the process' run id. 

* Worth mentioning because it tricked me for an embarrassingly long time. The method `multiprocessing.get_logger()`
  returns the package logger of multiprocessing. It's used to debug multi-process programs. Don't try to use it for your
  own logging else you'll get all the extra cruft from the underlying process management layer.

So, this module provides:

1) A 'LoggerThread' for logging in the background log-lines from sub-processes. Example usage is adding it to a main()
function before starting the processes:
```
    manager = multiprocessing.Manager()  # The manager for sharing objects between processes.
    logging_queue = manager.Queue()      # The queue used to send log-lines to the logger.
    logging_thread = simulation_logging.LoggerThread(logging_queue)  # Thread which will read the queue and log.
    logging_thread.start()

    # Start sub-processes any number of ways, pass the worker the logging_queue.
    pool = multiprocessing.Pool(num_processes)
    list(
        pool.imap_unordered(
            functools.partial(do_work, logging_queue=logging_queue),
            range(num_runs)
        )
    )

    # Stop the thread and cleanup.
    pool.close()
    pool_join()
    logging_thread.stop()
    logging_thread.join()
```


2. For each worker process, to be able to log and access the LoggerThread's queue, its signature should look like:
```
def do_work(run_id, logging_queue, log_level):
    # Initialize the sub-process logging, tag it with the run_id, and then use the returned logger. 
    logger = simulation_logging.get_logger(run_id, logging_queue, log_level)
    logger.info('I am alive!')
    ...
```

See: 
  https://docs.python.org/3/library/multiprocessing.html#logging

"""

# Used to prevent the race-condition of multiple parallel processes instantiating a logger.
logger_lock = multiprocessing.RLock()


class LoggerThread(threading.Thread):
    """Intended to be called within the main process, read messages from the queue, and then log them.

    The provided Queue should be passed to any sub-process workers, and in those workers, they should call `get_logger`.
    Doing so will funnel all sub-process logs to this thread where they will be written. This prevents logging
    collisions when writing to a single file.

    Based on the python multi-processing cookbook:
    https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes
    """
    STOP_SIGNAL = None

    def __init__(self, logging_queue: multiprocessing.Queue):
        super().__init__()
        self.logging_queue = logging_queue

    def run(self):
        while True:
            record = self.logging_queue.get()
            if record == self.STOP_SIGNAL:
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        logging.getLogger().debug('Logging thread is exiting.')

    def stop(self):
        self.logging_queue.put(self.STOP_SIGNAL)


class _SimulationAdapter(logging.LoggerAdapter):
    """A LoggerAdapter that injects the simulation and process id into log lines.

    This is used internally by `get_logger()`. Each sub-process gets its own adapter attached to the singleton
    QueueHandler.

    Log lines will look like
    `[INFO] [sim_id:1|pid:2374] Hello world!`

    See:
      https://docs.python.org/3/howto/logging-cookbook.html#using-loggeradapters-to-impart-contextual-information
    """
    def process(self, msg: Any, kwargs: MutableMapping[str, Any]) -> tuple[Any, MutableMapping[str, Any]]:
        return f'[sim_id:{self.extra["sim_id"]}] {msg}', kwargs


def get_logger(
        sim_id: int,
        logging_queue: multiprocessing.Queue,
        log_level: Union[int, str] = None) -> Union[logging.Logger, logging.LoggerAdapter]:
    """Return a Logger or LoggerAdapter, that can be used in a subprocess to safely write logs tagged with sim_id.

    This func should be called at the start of a multiprocessing.Process, and then all logging should be done through
    the returned object. All calls to the returned object are thread and process safe.

    Internally, we use a QueueHandler, which was designed for exactly this purpose. The current
    [python cookbook](https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes)
    for multiprocessing logging has each process implement a QueueHandler, but this seems to cause multiple-logging
    as each process's log goes to each QueueHandler. So this code makes sure there's exactly N=1 QueueHandlers in the
    subprocess space.

    :param sim_id: The identifier of the simulation that the process is running.
    :param logging_queue: A Queue that is connected to a ThreadLogger. All log records go through it.
    :param log_level: Optional logging level, if not provided, will log at INFO.
    :return: A Logger or LoggingAdapter that should be used for all logging in the process.
    """
    if logging_queue:
        try:
            logger_lock.acquire()  # Prevent a race condition of instantiating too many QueueHandlers.
            logger = logging.getLogger()
            if not logger.hasHandlers():
                queue_handler = QueueHandler(logging_queue)
                logging.getLogger().addHandler(queue_handler)
                if log_level:
                    logging.getLogger().setLevel(log_level)
        finally:
            logger_lock.release()
    else:
        logger = logging.getLogger()
    return _SimulationAdapter(logger, {'sim_id': sim_id})
