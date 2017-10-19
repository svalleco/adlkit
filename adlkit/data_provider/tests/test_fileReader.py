import Queue
import copy
import ctypes
import logging as lg
import multiprocessing
from unittest import TestCase

import numpy as np

from adlkit.data_provider.io_drivers import H5DataIODriver
from adlkit.data_provider.readers import FileReader

lg.basicConfig(level=lg.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s ')


class TestFileReader(TestCase):
    def test_read(self):
        """
        max_batches and batch_size are directly correlated to test input data
        
        make sure gen_rand_data has the correct shape
        
        :return: 
        """

        from mock_config import mock_batches, mock_expected_malloc_requests, \
            mock_class_index_map, mock_file_index_list

        mock_batches = copy.deepcopy(mock_batches)
        mock_expected_malloc_requests = copy.deepcopy(mock_expected_malloc_requests)
        mock_class_index_map = copy.deepcopy(mock_class_index_map)

        max_batches = len(mock_batches)
        batch_size = 500
        read_size = 2 * batch_size
        bucket_length = 10
        in_queue = multiprocessing.Queue(maxsize=max_batches)
        out_queue = multiprocessing.Queue(maxsize=max_batches)

        # building queue up with read requests
        for batch in mock_batches:
            try:
                in_queue.put(batch, timeout=1)
            except Queue.Full:
                pass

        reader_id = 0

        shared_data_pointer = range(bucket_length)

        for bucket in shared_data_pointer:
            data_sets = []
            for request in mock_expected_malloc_requests:
                # reshape the requested shape to match the batch_size
                shape = (read_size,) + request[1]

                shared_array_base = multiprocessing.Array(ctypes.c_double, np.prod(shape),
                                                          lock=False)
                shared_array = np.ctypeslib.as_array(shared_array_base)
                shared_array = shared_array.reshape(shape)
                data_sets.append(shared_array)

            state = multiprocessing.Value('i', 0)
            generator_start_counter = multiprocessing.Value('i', 0)
            generator_end_counter = multiprocessing.Value('i', 0)
            shared_data_pointer[bucket] = [state, data_sets, generator_start_counter,
                                           generator_end_counter]

        reader = FileReader(worker_id=reader_id, in_queue=in_queue, out_queue=out_queue,
                            shared_memory_pointer=shared_data_pointer,
                            max_batches=max_batches, read_size=read_size,
                            class_index_map=mock_class_index_map,
                            file_index_list=mock_file_index_list,
                            io_driver=H5DataIODriver())

        reader.read()

        out = list()
        for _ in range(max_batches):
            batch = None
            try:
                batch = out_queue.get(timeout=1)
            except Queue.Empty:
                pass
            finally:
                if batch is not None:
                    out.append(batch)

        self.assertEquals(len(out), max_batches,
                          "test consumed {0} of {1} expected batches from the in_queue".format(
                                  len(out),
                                  max_batches))

    def test_read_one_hot(self):
        from mock_config import mock_batches, mock_expected_malloc_requests, \
            mock_class_index_map, mock_one_hot, mock_file_index_list

        mock_batches = copy.deepcopy(mock_batches)
        mock_class_index_map = copy.deepcopy(mock_class_index_map)
        mock_one_hot = copy.deepcopy(mock_one_hot)
        mock_expected_malloc_requests = copy.deepcopy(mock_expected_malloc_requests)
        mock_expected_malloc_requests.extend(mock_one_hot)

        max_batches = len(mock_batches)
        batch_size = 500
        read_size = 2 * batch_size
        bucket_length = 10
        in_queue = multiprocessing.Queue(maxsize=max_batches)
        out_queue = multiprocessing.Queue(maxsize=max_batches)

        # building queue up with read requests
        for batch in mock_batches:
            try:
                in_queue.put(batch, timeout=1)
            except Queue.Full:
                pass

        reader_id = 0
        shared_data_pointer = range(bucket_length)

        for bucket in shared_data_pointer:
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
            generator_start_counter = multiprocessing.Value('i', 0)
            generator_end_counter = multiprocessing.Value('i', 0)
            shared_data_pointer[bucket] = [state, data_sets, generator_start_counter,
                                           generator_end_counter]

        reader = FileReader(worker_id=reader_id, in_queue=in_queue, out_queue=out_queue,
                            shared_memory_pointer=shared_data_pointer,
                            max_batches=max_batches, read_size=read_size,
                            class_index_map=mock_class_index_map,
                            make_one_hot=True, make_class_index=True,
                            file_index_list=mock_file_index_list,
                            io_driver=H5DataIODriver())

        reader.read()

        out = list()
        for _ in range(max_batches):
            batch = None
            try:
                batch = out_queue.get(timeout=1)
            except Queue.Empty:
                pass
            finally:
                if batch is not None:
                    out.append(batch)

        self.assertEquals(len(out), max_batches,
                          "test consumed {0} of {1} expected batches from the in_queue".format(
                                  len(out),
                                  max_batches))

    def test_list_read_descriptor(self):
        from mock_config import mock_filtered_batches, mock_expected_malloc_requests, \
            mock_class_index_map, mock_one_hot, mock_file_index_list

        mock_filtered_batches = copy.deepcopy(mock_filtered_batches)
        mock_class_index_map = copy.deepcopy(mock_class_index_map)
        mock_one_hot = copy.deepcopy(mock_one_hot)
        mock_expected_malloc_requests = copy.deepcopy(mock_expected_malloc_requests)
        mock_expected_malloc_requests.extend(mock_one_hot)

        max_batches = len(mock_filtered_batches)
        batch_size = 100
        read_size = 2 * batch_size
        bucket_length = 10
        in_queue = multiprocessing.Queue(maxsize=max_batches)
        out_queue = multiprocessing.Queue(maxsize=max_batches)

        # building queue up with read requests
        for batch in mock_filtered_batches:
            try:
                in_queue.put(batch, timeout=1)
            except Queue.Full:
                pass

        reader_id = 0
        shared_data_pointer = range(bucket_length)

        for bucket in shared_data_pointer:
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
            generator_start_counter = multiprocessing.Value('i', 0)
            generator_end_counter = multiprocessing.Value('i', 0)
            shared_data_pointer[bucket] = [state, data_sets, generator_start_counter,
                                           generator_end_counter]

        reader = FileReader(worker_id=reader_id, in_queue=in_queue, out_queue=out_queue,
                            shared_memory_pointer=shared_data_pointer,
                            max_batches=max_batches, read_size=read_size,
                            class_index_map=mock_class_index_map,
                            file_index_list=mock_file_index_list,
                            make_one_hot=True,
                            make_class_index=True,
                            io_driver=H5DataIODriver())

        reader.read()

        out = list()
        for _ in range(max_batches):
            batch = None
            try:
                batch = out_queue.get(timeout=1)
            except Queue.Empty:
                pass
            finally:
                if batch is not None:
                    out.append(batch)

        self.assertEquals(len(out), max_batches,
                          "test consumed {0} of {1} expected batches from the in_queue".format(
                                  len(out),
                                  max_batches))

        # def test_process_function(self):
        #     from mock_functions import mock_densifer_function
        #     from mock_config import mock_sparse_batches, mock_sparse_expected_malloc_requests_2,
        # mock_sparse_class_index_map
        #     mock_sparse_batches = copy.deepcopy(mock_sparse_batches)
        #     mock_sparse_expected_malloc_requests = copy.deepcopy(mock_sparse_expected_malloc_requests_2)
        #     mock_sparse_class_index_map = copy.deepcopy(mock_sparse_class_index_map)
        #
        #     max_batches = len(mock_sparse_batches)
        #     batch_size = 500
        #     read_size = 2 * batch_size
        #     bucket_length = len(mock_sparse_batches)
        #     in_queue = multiprocessing.Queue(maxsize=max_batches)
        #     out_queue = multiprocessing.Queue(maxsize=max_batches)
        #
        #     # building queue up with read requests
        #     for batch in mock_sparse_batches:
        #         try:
        #             in_queue.put(batch, timeout=1)
        #         except Queue.Full:
        #             pass
        #
        #     reader_id = 0
        #
        #     shared_data_pointer = range(bucket_length)
        #
        #     for bucket in shared_data_pointer:
        #         data_sets = []
        #         for request in mock_sparse_expected_malloc_requests:
        #             # reshape the requested shape to match the batch_size
        #             shape = (read_size,) + request[1]
        #
        #             shared_array_base = multiprocessing.Array(ctypes.c_double, np.prod(shape), lock=False)
        #             shared_array = np.ctypeslib.as_array(shared_array_base)
        #             shared_array = shared_array.reshape(shape)
        #             data_sets.append(shared_array)
        #
        #         state = multiprocessing.Value('i', 0)
        #         generator_start_counter = multiprocessing.Value('i', 0)
        #         generator_end_counter = multiprocessing.Value('i', 0)
        #         shared_data_pointer[bucket] = [state, data_sets, generator_start_counter, generator_end_counter]
        #
        #     reader = H5Reader(worker_id=reader_id, in_queue=in_queue, out_queue=out_queue,
        #                       shared_memory_pointer=shared_data_pointer,
        #                       max_batches=max_batches, read_size=read_size,
        #                       class_index_map=mock_sparse_class_index_map,
        #                       process_function=mock_densifer_function)
        #
        #     reader.read()
        #
        #     out = list()
        #     for _ in range(max_batches):
        #         batch = None
        #         try:
        #             batch = out_queue.get(timeout=1)
        #         except Queue.Empty:
        #             pass
        #         finally:
        #             if batch is not None:
        #                 out.append(batch)
        #
        #     self.assertEquals(len(out), max_batches,
        #                       "test consumed {0} of {1} expected batches from the in_queue".format(len(out),
        #                                                                                            max_batches))

    def test_make_file_index(self):
        from mock_config import mock_batches, mock_expected_malloc_requests, \
            mock_class_index_map, mock_file_index_malloc, mock_file_index_list

        mock_batches = copy.deepcopy(mock_batches)
        mock_class_index_map = copy.deepcopy(mock_class_index_map)
        mock_file_index_malloc = copy.deepcopy(mock_file_index_malloc)
        mock_expected_malloc_requests = copy.deepcopy(mock_expected_malloc_requests)
        mock_expected_malloc_requests.extend(mock_file_index_malloc)

        max_batches = len(mock_batches)
        batch_size = 500
        read_size = 2 * batch_size
        bucket_length = 10
        in_queue = multiprocessing.Queue(maxsize=max_batches)
        out_queue = multiprocessing.Queue(maxsize=max_batches)

        # building queue up with read requests
        for batch in mock_batches:
            try:
                in_queue.put(batch, timeout=1)
            except Queue.Full:
                pass

        reader_id = 0
        shared_data_pointer = range(bucket_length)

        for bucket in shared_data_pointer:
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
            generator_start_counter = multiprocessing.Value('i', 0)
            generator_end_counter = multiprocessing.Value('i', 0)
            shared_data_pointer[bucket] = [state, data_sets, generator_start_counter,
                                           generator_end_counter]

        reader = FileReader(worker_id=reader_id, in_queue=in_queue, out_queue=out_queue,
                            shared_memory_pointer=shared_data_pointer,
                            max_batches=max_batches,
                            read_size=read_size,
                            class_index_map=mock_class_index_map,
                            make_one_hot=True,
                            make_class_index=True,
                            make_file_index=True,
                            file_index_list=mock_file_index_list,
                            io_driver=H5DataIODriver())

        reader.read()

        out = list()
        for _ in range(max_batches):
            batch = None
            try:
                batch = out_queue.get(timeout=1)
            except Queue.Empty:
                pass
            finally:
                if batch is not None:
                    # TODO check batch for things
                    # file_tuple = batch[-1]
                    out.append(batch)

        self.assertEquals(len(out), max_batches,
                          "test consumed {0} of {1} expected batches from the in_queue".format(
                                  len(out),
                                  max_batches))
