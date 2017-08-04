import Queue
import copy
import ctypes
import logging as lg
import multiprocessing
from unittest import TestCase

import numpy as np

lg.basicConfig(level=lg.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s ')


class TestBaseWatcher(TestCase):
    def test_watch(self):

        lg.basicConfig(level=lg.DEBUG)

        from mock_config import mock_read_batches, mock_expected_malloc_requests

        mock_read_batches = copy.deepcopy(mock_read_batches)

        max_size = 5
        bucket_length = 10
        batch_size = 1000
        read_size = batch_size * 2
        reader_id = 0

        out_queue = multiprocessing.Queue(maxsize=max_size)

        shared_memory_pointer = [range(bucket_length)]

        for bucket in shared_memory_pointer[reader_id]:
            data_sets = []
            for request in mock_expected_malloc_requests:
                # TODO requests are not ordered!!!
                # TODO not sure it matters as long as its consistent
                # reshape the requested shape to match the batch_size
                shape = (read_size,) + request[1]

                shared_array_base = multiprocessing.Array(ctypes.c_double, np.prod(shape),
                                                          lock=False)
                shared_array = np.ctypeslib.as_array(shared_array_base)
                shared_array = shared_array.reshape(shape)
                data_sets.append(shared_array)

            state = multiprocessing.Value('i', 0)
            shared_memory_pointer[reader_id][bucket] = [state, data_sets]

        for batch in mock_read_batches:
            try:
                out_queue.put(batch)
            except Queue.Full:
                lg.debug("CRITICAL, CANNOT PUT BATCHES IN OUT_QUEUE")
                max_size -= 1

        multicast_queues = range(5)

        for queue_index in multicast_queues:
            multicast_queues[queue_index] = multiprocessing.Queue(maxsize=max_size)

            # tmp_watcher = BaseWatcher(worker_id=1000, shared_memory_pointer=shared_memory_pointer, )
            # TODO complete or throw away, test is already in file_data_provider
