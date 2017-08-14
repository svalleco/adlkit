import Queue
import logging as lg
import multiprocessing
import time

import numpy as np

from .config import STOP_MESSAGE

worker_log = lg.getLogger('data_provider.workers.worker')


class WorkerError(Exception):
    worker_id = None

    def __init__(self, message, worker_id):
        super(WorkerError, self).__init__(message)

        self.worker_id = worker_id


# def report_error(run_function):
#     def wrapper():
#         try:
#             run_function()
#         except WorkerError:


class Worker(multiprocessing.Process):
    def __init__(self, worker_id, control_queue_depth=1, sleep_duration=1,
                 **kwargs):
        super(Worker, self).__init__()

        np.random.seed()

        self.control_queue = multiprocessing.Queue(maxsize=control_queue_depth)
        self.worker_id = worker_id

        self.stop = multiprocessing.Value('i', 0)
        self.stop_check = False

        self.batch_count = 0
        self.file_handle_holder = dict()
        self.sleep_duration = sleep_duration

    def run(self, **kwargs):
        return

    def send_command(self, payload, block=True):
        try:
            self.control_queue.put(payload, block=block)
            return True
        except Queue.Full:
            # self.sleep()
            # worker_log.debug(" *{0}* command queue full".format(self.worker_id))
            return False

    def get_command(self, block=True):
        try:
            return self.control_queue.get(block=block)
        except Queue.Empty:
            # worker_log.debug(" *{0}* command queue empty".format(self.worker_id))
            # self.sleep()
            return None

    def get_all_commands(self):
        while True:
            tmp = self.get_command(block=False)
            if tmp is None:
                return
            elif tmp == STOP_MESSAGE:
                # self.hard_stop.set()
                self.stop_check = True

    def should_stop(self):
        self.get_all_commands()
        return self.stop_check

    def seppuku(self):
        for file_handle in self.file_handle_holder:
            try:
                self.file_handle_holder[file_handle].close()
            except Exception as e:
                lg.debug(e)

        with self.stop.get_lock():
            self.stop.value = 1

    def debug(self, message):
        # if isinstance(message, list):
        #     message = " ".join(message)
        # lg.info('{0} {1}'.format(time.time(), message))

        pass

    def sleep(self):
        if self.sleep_duration is not None:
            time.sleep(self.sleep_duration)
