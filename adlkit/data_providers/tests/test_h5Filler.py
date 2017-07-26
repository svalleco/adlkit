import Queue
import copy
import logging as lg
import multiprocessing
from unittest import TestCase

from adlkit.data_providers.fillers import H5Filler

lg.basicConfig(level=lg.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s ')
test_logger = lg.getLogger('data_providers.tests')


class TestH5Filler(TestCase):
    def test_fill(self):
        from mock_config import mock_classes, mock_class_index_map, \
            mock_data_sets, mock_file_index_list
        mock_classes = copy.deepcopy(mock_classes)
        mock_class_index_map = copy.deepcopy(mock_class_index_map)
        mock_data_sets = copy.deepcopy(mock_data_sets)

        max_batches = 10
        batch_size = 500
        read_size = batch_size * 2
        in_queue = multiprocessing.Queue(maxsize=max_batches)
        malloc_queue = multiprocessing.Queue(maxsize=max_batches)

        filler = H5Filler(classes=mock_classes,
                          class_index_map=mock_class_index_map,
                          in_queue=in_queue, worker_id=1, read_size=read_size,
                          max_batches=max_batches, malloc_queue=malloc_queue,
                          data_sets=mock_data_sets,
                          file_index_list=mock_file_index_list,
                          wrap_examples=True)

        filler.fill()

        out = list()
        for _ in range(max_batches):
            batch = None
            try:
                batch = in_queue.get(timeout=1)
            except Queue.Empty:
                pass
            finally:
                if batch is not None:
                    out.append(batch)
                    count = 0
                    for item in batch:
                        count += item[3][1] - item[3][0]

                    self.assertEquals(count, read_size,
                                      "read_batch was returned with read_size {0}, instead of batch_size {1}".format(
                                          count,
                                          batch_size))

        self.assertEquals(len(out), max_batches,
                          "test consumed {0} of {1} expected batches from the in_queue".format(
                              len(out), max_batches))

    def test_inform_data_provider(self):
        from mock_config import mock_class_index_map, mock_classes, \
            mock_data_sets, mock_expected_malloc_requests, mock_file_index_list
        mock_classes = copy.deepcopy(mock_classes)
        mock_class_index_map = copy.deepcopy(mock_class_index_map)
        mock_data_sets = copy.deepcopy(mock_data_sets)
        mock_file_index_list = copy.deepcopy(mock_file_index_list)

        max_batches = 5
        batch_size = 200
        read_size = batch_size * 2
        in_queue = multiprocessing.Queue(maxsize=max_batches)
        malloc_queue = multiprocessing.Queue(maxsize=max_batches)

        filler = H5Filler(classes=mock_classes,
                          class_index_map=mock_class_index_map,
                          in_queue=in_queue, worker_id=1, read_size=read_size,
                          max_batches=max_batches, malloc_queue=malloc_queue,
                          data_sets=mock_data_sets,
                          file_index_list=mock_file_index_list)

        filler.fill()

        out = list()
        for _ in range(max_batches):
            malloc_request = None
            try:
                malloc_request = malloc_queue.get(timeout=1)
            except Queue.Empty:
                pass
            finally:
                if malloc_request is not None:
                    out.append(malloc_request)

        for request, expected_request in zip(out,
                                             mock_expected_malloc_requests):
            self.assertEqual(request, expected_request)

    def test_fill_with_wrap(self):
        """
        testing with 3 files, 3 classes, w/ shape (1000,5)
        :return: 
        """
        from mock_config import mock_classes, mock_class_index_map, \
            mock_data_sets, mock_file_index_list
        mock_classes = copy.deepcopy(mock_classes)
        mock_class_index_map = copy.deepcopy(mock_class_index_map)
        mock_data_sets = copy.deepcopy(mock_data_sets)
        mock_file_index_list = copy.deepcopy(mock_file_index_list)

        max_batches = 10
        batch_size = 500
        read_size = batch_size * 2
        in_queue = multiprocessing.Queue(maxsize=max_batches)
        malloc_queue = multiprocessing.Queue(maxsize=max_batches)

        filler = H5Filler(classes=mock_classes,
                          class_index_map=mock_class_index_map,
                          in_queue=in_queue,
                          worker_id=1, read_size=read_size,
                          max_batches=max_batches, malloc_queue=malloc_queue,
                          wrap_examples=True, data_sets=mock_data_sets,
                          file_index_list=mock_file_index_list)

        filler.fill()

        out = int()
        for _ in range(max_batches):
            batch = None
            try:
                batch = in_queue.get(timeout=1)
            except Queue.Empty:
                pass
            finally:
                if batch is not None:
                    out += 1
                    count = 0
                    for item in batch:
                        count += item[3][1] - item[3][0]

                    self.assertEquals(count, read_size,
                                      "read_batch was returned with read_size {0}, instead of batch_size {1} \n {2}".format(
                                          count,
                                          read_size,
                                          batch))

        self.assertEquals(out, max_batches,
                          "test consumed {0} of {1} expected batches from the in_queue".format(
                              out, max_batches))

    def test_fill_without_wrap(self):
        """
        testing with 3 files, 3 classes, w/ shape (1000,5)
        
        expected_batches is directly correlated to input batches and expected examples per
        class
        
        :return: 
        """
        from adlkit.data_providers.tests.mock_config import mock_classes, mock_class_index_map, \
            mock_data_sets,mock_file_index_list
        mock_classes = copy.deepcopy(mock_classes)
        mock_class_index_map = copy.deepcopy(mock_class_index_map)
        mock_data_sets = copy.deepcopy(mock_data_sets)
        mock_file_index_list = copy.deepcopy(mock_file_index_list)

        expected_batches = 2
        max_batches = 4
        batch_size = 500
        read_size = batch_size * 2
        in_queue = multiprocessing.Queue(maxsize=max_batches)
        malloc_queue = multiprocessing.Queue(maxsize=max_batches)

        filler = H5Filler(classes=mock_classes,
                          class_index_map=mock_class_index_map,
                          in_queue=in_queue,
                          worker_id=1, read_size=read_size,
                          max_batches=max_batches, malloc_queue=malloc_queue,
                          wrap_examples=False, data_sets=mock_data_sets,
                          file_index_list=mock_file_index_list)

        filler.fill()

        out = int()
        for _ in range(max_batches):
            batch = None
            try:
                batch = in_queue.get(timeout=1)
            except Queue.Empty:
                pass
            finally:
                if batch is not None:
                    out += 1
                    count = 0
                    for item in batch:
                        count += item[3][1] - item[3][0]

                    self.assertEquals(count, read_size,
                                      "read_batch was returned with read_size {0}, instead of batch_size {1} \n {2}".format(
                                          count,
                                          read_size,
                                          batch))

        self.assertEquals(out, expected_batches,
                          "test consumed {0} of {1} expected batches from the in_queue".format(
                              out, expected_batches))

    def test_filter_function(self):
        from mock_functions import mock_filter_function
        from mock_config import mock_classes, mock_class_index_map, \
            mock_data_sets, mock_file_index_list
        mock_classes = copy.deepcopy(mock_classes)
        mock_class_index_map = copy.deepcopy(mock_class_index_map)
        mock_data_sets = copy.deepcopy(mock_data_sets)

        max_batches = 3
        batch_size = 100
        read_size = batch_size * 2
        in_queue = multiprocessing.Queue(maxsize=max_batches)
        malloc_queue = multiprocessing.Queue(maxsize=max_batches)

        filler = H5Filler(classes=mock_classes,
                          class_index_map=mock_class_index_map,
                          in_queue=in_queue,
                          worker_id=1, read_size=read_size,
                          max_batches=max_batches, malloc_queue=malloc_queue,
                          wrap_examples=False, data_sets=mock_data_sets,
                          filter_function=mock_filter_function,
                          file_index_list=mock_file_index_list)

        filler.fill()

        out = 0
        meow = list()
        for _ in range(max_batches):
            batch = None
            try:
                batch = in_queue.get(timeout=1)
            except Queue.Empty:
                pass
            finally:
                if batch is not None:
                    meow.append(batch)
                    out += 1
                    count = 0
                    for item in batch:
                        count += len(item[3])

                    self.assertEquals(count, read_size,
                                      "read_batch was returned with read_size {0}, instead of batch_size {1} \n {2}".format(
                                          count,
                                          read_size,
                                          batch))

        self.assertEquals(out, max_batches,
                          "test consumed {0} of {1} expected batches from the in_queue".format(
                              out, max_batches))
