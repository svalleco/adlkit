"""
__author__ = 'W. Grayson Hilliard'
"""
from __future__ import absolute_import

import Queue
import copy
import logging as lg
import os
from unittest import TestCase

from adlkit.data_provider.data_providers import FileDataProvider
from adlkit.data_provider.fillers import FileFiller
from adlkit.data_provider.generators import BaseGenerator
from adlkit.data_provider.io_drivers import H5DataIODriver
from adlkit.data_provider.readers import FileReader
from adlkit.data_provider.watchers import BaseWatcher

lg.basicConfig(level=lg.DEBUG, format='%(asctime)s %(levelname)s %(name)s %(message)s ')
test_logger = lg.getLogger('data_provider.tests')

sleep_duration = 0.05


class TestFileDataProvider(TestCase):
    def process_sample_specification(self):
        from adlkit.data_provider.tests.mock_config import mock_sample_specification
        mock_sample_specification = copy.deepcopy(mock_sample_specification)

        tmp_data_provider = FileDataProvider(mock_sample_specification,
                                             # batch_size=100,
                                             n_readers=4)

        self.assertEqual(len(tmp_data_provider.config.data_sets), 2)
        self.assertEqual(len(tmp_data_provider.config.classes), 3)
        self.assertEqual(len(tmp_data_provider.config.class_index_map), 3)

    def test_process_malloc_requests(self):
        from adlkit.data_provider.tests.mock_config import mock_sample_specification, \
            mock_expected_malloc_requests
        mock_sample_specification = copy.deepcopy(mock_sample_specification)
        mock_expected_malloc_requests = copy.deepcopy(
                mock_expected_malloc_requests)

        tmp_data_provider = FileDataProvider(mock_sample_specification,
                                             batch_size=100,
                                             n_readers=4,
                                             sleep_duration=sleep_duration)

        tmp_data_provider.start_queues()
        tmp_data_provider.start_filler(FileFiller,
                                       io_driver=H5DataIODriver())

        tmp_data_provider.process_malloc_requests()

        for request, expected_request in zip(tmp_data_provider.malloc_requests,
                                             mock_expected_malloc_requests):
            self.assertEqual(request, expected_request)

    def test_make_shared_malloc(self):
        from adlkit.data_provider.tests.mock_config import mock_sample_specification, \
            mock_expected_malloc_requests
        mock_sample_specification = copy.deepcopy(mock_sample_specification)
        mock_expected_malloc_requests = copy.deepcopy(
                mock_expected_malloc_requests)

        batch_size = 100

        tmp_data_provider = FileDataProvider(mock_sample_specification,
                                             batch_size=batch_size,
                                             n_readers=4)

        tmp_data_provider.malloc_requests = mock_expected_malloc_requests

        tmp_data_provider.worker_count = 1

        worker_id = 0

        tmp_data_provider.make_shared_malloc(worker_id)

        bucket = 1
        data_set = 0
        lock = 0

        self.assertEqual(len(tmp_data_provider.shared_memory), 1,
                         "shared memory was not extended correctly ")

        # check to make sure all buckets were allocated
        self.assertEqual(len(tmp_data_provider.shared_memory[worker_id]), 10,
                         "shared memory buckets were not allocated correctly")

        # check to make sure the shape matches out expected value

        self.assertEqual(
                tmp_data_provider.shared_memory[worker_id][bucket][lock + 1][
                    data_set].shape, (100, 5),
                "shared memory shape doesn't match")

    def test_filler_to_malloc(self):
        from adlkit.data_provider.tests.mock_config import mock_sample_specification, \
            mock_expected_malloc_requests
        mock_sample_specification = copy.deepcopy(mock_sample_specification)
        mock_expected_malloc_requests = copy.deepcopy(
                mock_expected_malloc_requests)

        batch_size = 100

        tmp_data_provider = FileDataProvider(mock_sample_specification,
                                             batch_size=batch_size,
                                             n_readers=4,
                                             sleep_duration=sleep_duration)

        tmp_data_provider.start_queues()
        tmp_data_provider.start_filler(FileFiller,
                                       io_driver=H5DataIODriver(),
                                       shape_reader_io_driver=None)

        tmp_data_provider.process_malloc_requests()

        tmp_data_provider.worker_count = worker_id = 1

        tmp_data_provider.make_shared_malloc(worker_id)

        bucket = 0
        data_set = 0
        lock = 0
        # TODO !!possible off by one error, needs logic check!!

        # check to make sure the shared mem was extended
        self.assertEqual(len(tmp_data_provider.shared_memory),
                         tmp_data_provider.worker_count + 1,
                         "shared memory was not extended correctly ")

        # check to make sure all buckets were allocated
        self.assertEqual(len(tmp_data_provider.shared_memory[worker_id]), 10,
                         "shared memory buckets were not allocated correctly")

        # Multiple Generator start and end locks
        self.assertEqual(
                len(tmp_data_provider.shared_memory[worker_id][bucket]), 4,
                "shared memory locks were not set correctly")

        # check to make sure all data sets were allocated
        self.assertEqual(
                len(tmp_data_provider.shared_memory[worker_id][bucket][lock + 1]),
                len(mock_expected_malloc_requests),
                "shared memory data sets were not allocated correctly")

        # check to make sure the shape matches out expected value
        self.assertEqual(
                tmp_data_provider.shared_memory[worker_id][bucket][lock + 1][
                    data_set].shape, (100, 5),
                "shared memory shape doesn't match")

    def test_start_filler(self):
        from adlkit.data_provider.tests.mock_config import mock_sample_specification
        mock_sample_specification = copy.deepcopy(mock_sample_specification)

        tmp_data_provider = FileDataProvider(mock_sample_specification,
                                             batch_size=100,
                                             read_multiplier=2,
                                             n_readers=4,
                                             sleep_duration=sleep_duration)

        tmp_data_provider.start_queues()
        tmp_data_provider.start_filler(FileFiller,
                                       io_driver=H5DataIODriver())

        max_batches = 10

        out = list()
        while True:
            if len(out) == max_batches:
                break
            batch = None
            try:
                batch = tmp_data_provider.in_queue.get(timeout=1)
            except Queue.Empty:
                pass
            finally:
                if batch is not None:
                    out.append(batch)
                    count = 0
                    for item in batch:
                        count += item[3][1] - item[3][0]

                    self.assertEquals(count, tmp_data_provider.config.read_size,
                                      "batch was returned with batch_size {0}, instead of batch_size {1}".format(
                                              count,
                                              tmp_data_provider.config.batch_size))

        self.assertEquals(len(out), max_batches,
                          "test consumed {0} of {1} expected batches from the in_queue".format(
                                  len(out), max_batches))

    def test_start_reader(self):
        from adlkit.data_provider.tests.mock_config import mock_sample_specification, \
            mock_batches, mock_expected_malloc_requests
        mock_sample_specification = copy.deepcopy(mock_sample_specification)
        mock_batches = copy.deepcopy(mock_batches)
        mock_expected_malloc_requests = copy.deepcopy(
                mock_expected_malloc_requests)

        tmp_data_provider = FileDataProvider(mock_sample_specification,
                                             batch_size=500,
                                             read_multiplier=2,
                                             make_one_hot=True,
                                             make_class_index=True,
                                             sleep_duration=sleep_duration)

        tmp_data_provider.start_queues()

        # inserting mock data into the queue
        for batch in mock_batches:
            try:
                tmp_data_provider.in_queue.put(batch, timeout=1)
            except Queue.Full:
                pass

        tmp_data_provider.malloc_requests = mock_expected_malloc_requests

        reader_id = tmp_data_provider.start_reader(FileReader,
                                                   io_driver=H5DataIODriver())
        # lg.debug("I am expecting to write to {0}".format(hex(id(tmp_data_provider.shared_memory[0][0][1][0]))))
        max_batches = 5

        out = list()
        while True:
            if len(out) == max_batches:
                break
            batch = None
            try:
                batch = tmp_data_provider.out_queue.get(timeout=1)
            except Queue.Empty:
                pass
            finally:
                if batch is not None:
                    out.append(batch)

        for item in out:
            tmp_worker_id, tmp_bucket_index, tmp_data_sets, batch_id = item
            # Check that each bucket was successfully updated
            self.assertEqual(tmp_data_provider.shared_memory[tmp_worker_id][
                                 tmp_bucket_index][0].value, 1)

        # check for correct reader_id assignment
        self.assertEqual(len(tmp_data_provider.readers), reader_id + 1)

        # check that mock data successfully was processed
        self.assertEquals(len(out), max_batches,
                          "test consumed {0} of {1} expected batches from the out_queue".format(
                                  len(out), max_batches))

    def test_start_reader_one_hot(self):
        from adlkit.data_provider.tests.mock_config import mock_sample_specification, \
            mock_batches, mock_expected_malloc_requests
        mock_sample_specification = copy.deepcopy(mock_sample_specification)
        mock_batches = copy.deepcopy(mock_batches)
        mock_expected_malloc_requests = copy.deepcopy(
                mock_expected_malloc_requests)

        tmp_data_provider = FileDataProvider(mock_sample_specification,
                                             batch_size=500,
                                             read_multiplier=2,
                                             sleep_duration=sleep_duration)

        tmp_data_provider.start_queues()
        max_batches = 5
        # inserting mock data into the queue
        for batch in mock_batches:
            try:
                tmp_data_provider.in_queue.put(batch, timeout=1)
            except Queue.Full:
                lg.debug("CRITICAL, CANNOT FILL QUEUE WITH BATCHES, "
                         "DECRIMENTING max_batches")
                max_batches -= 1

        tmp_data_provider.malloc_requests = mock_expected_malloc_requests

        reader_id = tmp_data_provider.start_reader(FileReader,
                                                   io_driver=H5DataIODriver())
        # lg.debug("I am expecting to write to {0}".format(hex(id(tmp_data_provider.shared_memory[0][0][1][0]))))

        out = list()
        while True:
            if len(out) == max_batches:
                break
            batch = None
            try:
                batch = tmp_data_provider.out_queue.get(timeout=1)
            except Queue.Empty:
                pass
            finally:
                if batch is not None:
                    out.append(batch)

        for item in out:
            tmp_worker_id, tmp_bucket_index, tmp_data_sets, batch_id = item
            # Check that each bucket was successfully updated
            self.assertEqual(tmp_data_provider.shared_memory[tmp_worker_id][
                                 tmp_bucket_index][0].value, 1)

        # check for correct reader_id assignment
        self.assertEqual(len(tmp_data_provider.readers), reader_id + 1)

        # check that mock data successfully was processed
        self.assertEquals(len(out), max_batches,
                          "test consumed {0} of {1} expected batches from the out_queue".format(
                                  len(out), max_batches))

    def test_generator(self):
        from adlkit.data_provider.tests.mock_config import mock_sample_specification, mock_batches, \
            mock_expected_malloc_requests, mock_file_index_list, mock_class_index_map
        mock_sample_specification = copy.deepcopy(mock_sample_specification)
        mock_batches = copy.deepcopy(mock_batches)
        mock_expected_malloc_requests = copy.deepcopy(mock_expected_malloc_requests)
        mock_file_index_list = copy.deepcopy(mock_file_index_list)
        mock_class_index_map = copy.deepcopy(mock_class_index_map)

        tmp_data_provider = FileDataProvider(mock_sample_specification,
                                             batch_size=500,
                                             read_multiplier=2,
                                             n_readers=3,
                                             sleep_duration=sleep_duration)

        tmp_data_provider.start_queues()
        max_batches = 5
        # max_batches = 100
        # inserting mock data into the queue (should probably be a function at this point)
        for batch in mock_batches:
            try:
                tmp_data_provider.in_queue.put(batch, timeout=1)
            except Queue.Full:
                max_batches -= 1

        tmp_data_provider.malloc_requests = mock_expected_malloc_requests

        tmp_data_provider.start_reader(FileReader,
                                       io_driver=H5DataIODriver())
        # lg.debug("I am expecting to write to {0}".format(hex(id(tmp_data_provider.shared_memory[0][0][1][0]))))

        this = BaseGenerator(out_queue=tmp_data_provider.out_queue,
                             batch_size=tmp_data_provider.config.batch_size,
                             read_size=tmp_data_provider.config.read_size,
                             max_batches=max_batches,
                             shared_memory_pointer=tmp_data_provider.shared_memory,
                             file_index_list=mock_file_index_list,
                             class_index_map=mock_class_index_map)

        batch_size = tmp_data_provider.config.batch_size
        for _ in range(max_batches):
            tmp = this.generate().next()
            self.assertEqual(len(tmp), 2)
            self.assertEqual(tmp[0].shape, (batch_size, 5))
            self.assertEqual(tmp[1].shape, (batch_size, 5))

    def test_generator_one_hot_and_class_index(self):
        from adlkit.data_provider.tests.mock_config import mock_sample_specification, \
            mock_batches, mock_expected_malloc_requests, mock_file_index_list, mock_class_index_map
        mock_class_index_map = copy.deepcopy(mock_class_index_map)
        mock_sample_specification = copy.deepcopy(mock_sample_specification)
        mock_batches = copy.deepcopy(mock_batches)
        mock_expected_malloc_requests = copy.deepcopy(mock_expected_malloc_requests)
        mock_file_index_list = copy.deepcopy(mock_file_index_list)

        batch_size = 500
        tmp_data_provider = FileDataProvider(mock_sample_specification,
                                             batch_size=batch_size,
                                             read_multiplier=2,
                                             make_one_hot=True,
                                             make_class_index=True,
                                             sleep_duration=sleep_duration)

        tmp_data_provider.start_queues()
        max_batches = 5
        # inserting mock data, once again
        for batch in mock_batches:
            try:
                tmp_data_provider.in_queue.put(batch, timeout=1)
            except Queue.Full:
                max_batches -= 1

        tmp_data_provider.malloc_requests = mock_expected_malloc_requests

        tmp_data_provider.start_reader(FileReader,
                                       io_driver=H5DataIODriver())
        # lg.debug("I am expecting to write to {0}".format(hex(id(tmp_data_provider.shared_memory[0][0][1][0]))))

        this = BaseGenerator(out_queue=tmp_data_provider.out_queue,
                             batch_size=tmp_data_provider.config.batch_size,
                             read_size=tmp_data_provider.config.read_size,
                             max_batches=max_batches,
                             shared_memory_pointer=tmp_data_provider.shared_memory,
                             file_index_list=mock_file_index_list,
                             class_index_map=mock_class_index_map)

        count = 0
        for tmp in this.generate():
            count += 1
            self.assertEqual(len(tmp), 4)
            self.assertEqual(tmp[0].shape, (batch_size, 5))
            self.assertEqual(tmp[1].shape, (batch_size, 5))
            self.assertEqual(tmp[2].shape, (batch_size,))
            self.assertEqual(tmp[3].shape, (batch_size, 3))

        self.assertEqual(count, max_batches)

    def test_end_to_end(self):
        from adlkit.data_provider.tests.mock_config import mock_sample_specification
        mock_sample_specification = copy.deepcopy(mock_sample_specification)

        batch_size = 5
        max_batches = 5
        tmp_data_provider = FileDataProvider(mock_sample_specification,
                                             batch_size=batch_size,
                                             read_multiplier=2,
                                             make_one_hot=True,
                                             make_class_index=True,
                                             make_file_index=True,
                                             n_readers=4,
                                             sleep_duration=sleep_duration,
                                             max_batches=max_batches)

        tmp_data_provider.start_queues()
        tmp_data_provider.start_filler(FileFiller,
                                       io_driver=H5DataIODriver())

        tmp_data_provider.process_malloc_requests()

        tmp_data_provider.start_reader(FileReader,
                                       io_driver=H5DataIODriver())

        # lg.debug("I am expecting to write to {0}".format(hex(id(tmp_data_provider.shared_memory[0][0][1][0]))))

        generator_id = tmp_data_provider.start_generator(BaseGenerator)

        count = 0
        for tmp in tmp_data_provider.generators[generator_id].generate():
            count += 1
            self.assertEqual(len(tmp), 5)
            self.assertEqual(tmp[0].shape, (batch_size, 5))
            self.assertEqual(tmp[1].shape, (batch_size, 5))
            self.assertEqual(tmp[2].shape, (batch_size,))
            self.assertEqual(tmp[3].shape, (batch_size, 3))
            self.assertEqual(len(tmp[4][0]), 2)

        self.assertEqual(count, max_batches,
                         "we have a leaky generator here {0} != {1}".format(
                                 count, max_batches))

    def test_stop_queues(self):
        from adlkit.data_provider.tests.mock_config import mock_sample_specification
        mock_sample_specification = copy.deepcopy(mock_sample_specification)

        tmp_file_data_provider = FileDataProvider(mock_sample_specification)
        tmp_file_data_provider.start_queues()
        tmp_file_data_provider.stop_queues()

        self.assertIsNone(tmp_file_data_provider.in_queue,
                          "in_queue did not close")
        self.assertIsNone(tmp_file_data_provider.out_queue,
                          "out_queue did not close")
        self.assertIsNone(tmp_file_data_provider.malloc_queue,
                          "malloc_queue did not close")
        self.assertEqual(len(tmp_file_data_provider.multicast_queues), 0,
                         "malloc_queue did not close")

        del tmp_file_data_provider

    def test_start(self):
        from adlkit.data_provider.tests.mock_config import mock_sample_specification
        mock_sample_specification = copy.deepcopy(mock_sample_specification)

        max_batches = 100
        batch_size = 5

        tmp_data_provider = FileDataProvider(mock_sample_specification,
                                             batch_size=batch_size,
                                             read_multiplier=2,
                                             make_one_hot=True,
                                             make_class_index=True,
                                             n_readers=4,
                                             wrap_examples=True,
                                             sleep_duration=sleep_duration)

        tmp_data_provider.start(filler_class=FileFiller,
                                reader_class=FileReader,
                                generator_class=BaseGenerator)

        # I could make this a function like the above, but it makes sense not to limit
        # the full pipeline with max_batches. Tentatively there is no way to pass parameters
        # to certain parts of the pipeline. Everyone gets every kwarg, for better or for worse.
        generator_id = 0
        for _ in range(max_batches):
            # TODO better checks
            this = tmp_data_provider.generators[generator_id].generate().next()
            self.assertEqual(len(this), 4)

    def test_start_then_stop(self):
        """
        if this fails, it's most likely due to a lack of allowed file descriptors
        :return: 
        """

        from adlkit.data_provider.tests.mock_config import mock_sample_specification
        mock_sample_specification = copy.deepcopy(mock_sample_specification)

        batch_size = 5

        tmp_data_provider = FileDataProvider(mock_sample_specification,
                                             batch_size=batch_size,
                                             read_batches_per_epoch=1000,
                                             read_multiplier=1,
                                             make_one_hot=True,
                                             make_class_index=True,
                                             wrap_examples=True,
                                             n_readers=5,
                                             n_buckets=2,
                                             q_multipler=3,
                                             sleep_duration=sleep_duration
                                             )

        tmp_data_provider.start(filler_class=FileFiller,
                                reader_class=FileReader,
                                generator_class=BaseGenerator)
        generator_id = 0

        for _ in range(100):
            # for _ in range(10000000):
            tmp = tmp_data_provider.generators[generator_id].generate().next()
            # TODO better checks
            self.assertEqual(len(tmp), 4)

        tmp_data_provider.hard_stop()

        # we are checking the that readers are killing themselves
        # by checking that the pid no longer exists
        # https://stackoverflow.com/questions/568271/how-to-check-if-there-exists-a-process-with-a-given-pid-in-python
        for reader_process in tmp_data_provider.readers:
            this = None
            try:
                os.kill(reader_process.pid, 0)
                this = True
            except OSError:
                this = False
            finally:
                self.assertEqual(this, False)

    def test_multiple_generators(self):
        from adlkit.data_provider.tests.mock_config import mock_sample_specification
        mock_sample_specification = copy.deepcopy(mock_sample_specification)

        max_batches = 100
        batch_size = 100
        n_generators = 5

        tmp_data_provider = FileDataProvider(mock_sample_specification,
                                             batch_size=batch_size,
                                             read_multiplier=2,
                                             make_one_hot=True,
                                             make_class_index=True,
                                             n_readers=3,
                                             n_generators=n_generators,
                                             wrap_examples=True,
                                             sleep_duration=sleep_duration)

        tmp_data_provider.start(filler_class=FileFiller,
                                reader_class=FileReader,
                                generator_class=BaseGenerator,
                                watcher_class=BaseWatcher)

        # ugly, but mostly necessary
        generator_counter = [0] * n_generators
        total_count = 0
        for loop in range(max_batches):
            for gen_index in range(len(tmp_data_provider.generators)):
                this = None
                this = tmp_data_provider.generators[gen_index].generate().next()

                self.assertEqual(len(this), 4)
                generator_counter[gen_index] += 1
                total_count += 1

        for generator_count in generator_counter:
            self.assertEqual(generator_count, max_batches)

        self.assertEqual(total_count, max_batches * n_generators)

    def test_watcher_multicast(self):
        from adlkit.data_provider.tests.mock_config import mock_read_batches, \
            mock_sample_specification, mock_expected_malloc_requests
        mock_read_batches = copy.deepcopy(mock_read_batches)
        mock_sample_specification = copy.deepcopy(mock_sample_specification)
        mock_expected_malloc_requests = copy.deepcopy(
                mock_expected_malloc_requests)

        batch_size = 100
        max_batches = 5
        n_generators = 5
        n_readers = 5
        tmp_data_provider = FileDataProvider(mock_sample_specification,
                                             batch_size=batch_size,
                                             read_multipler=2,
                                             make_one_hot=True,
                                             make_class_index=True,
                                             n_readers=n_readers,
                                             n_generators=n_generators,
                                             n_buckets=2,
                                             sleep_duration=sleep_duration)

        tmp_data_provider.start_queues()
        tmp_data_provider.malloc_requests = mock_expected_malloc_requests
        tmp_data_provider.make_shared_malloc(0)

        for batch in mock_read_batches:
            try:
                tmp_data_provider.out_queue.put(batch, timeout=0)
            except Queue.Full:
                max_batches -= 1

        tmp_watcher = BaseWatcher(worker_id=0,
                                  shared_memory_pointer=tmp_data_provider.shared_memory,
                                  multicast_queues=tmp_data_provider.multicast_queues,
                                  max_batches=max_batches,
                                  out_queue=tmp_data_provider.out_queue)

        tmp_watcher.watch()

        gen_counter = [0] * 5

        for loop in range(max_batches):
            for gen_index in range(len(tmp_data_provider.multicast_queues)):
                this = None
                try:
                    this = tmp_data_provider.multicast_queues[gen_index].get()
                except Queue.Empty:
                    pass
                finally:
                    if this is not None:
                        gen_counter[gen_index] += 1

        for gen_count in gen_counter:
            self.assertEqual(gen_count, max_batches)

            # def test_watcher_free(self):
            #     import logging as lg
            #
            #     lg.basicConfig(level=lg.DEBUG)
            #
            #     import copy
            #     from tests.mock_config import mock_sample_specification
            #     from threaded_gen_two import FileDataProvider, BaseWatcher, BaseGenerator, H5Reader, H5Filler
            #
            #     mock_sample_specification = copy.deepcopy(mock_sample_specification)
            #
            #     batch_size = 100
            #     max_batches = 5
            #     n_generators = 5
            #     tmp_data_provider = FileDataProvider(mock_sample_specification,
            #                                          batch_size=batch_size,
            #                                          read_multipler=2,
            #                                          make_one_hot=True,
            #                                          make_class_index=True,
            #                                          n_readers=5,
            #                                          n_generators=n_generators)
            #
            #     tmp_data_provider.start(filler_class=H5Filler,
            #                             reader_class=H5Reader,
            #                             generator_class=BaseGenerator,
            #                             watcher_class=BaseWatcher)
            #     for _ in range(100):
            #         for generator_index in range(tmp_data_provider.config.n_generators):
            #             tmp_data_provider.generators[generator_index].generate().next()
            #             lg.debug(tmp_data_provider.shared_memory[0][0][2].value)
