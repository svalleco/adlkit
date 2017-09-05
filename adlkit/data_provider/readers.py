import Queue
import collections
import logging as lg
import time

import h5py
import keras
import numpy as np
from six import raise_from

from .config import READER_OFFSET
from .workers import Worker

# lg.basicConfig(level=lg.INFO)

reader_logger = lg.getLogger('data_provider.workers.readers')


class BaseReader(Worker):
    """

    """

    def __init__(self, worker_id, in_queue, out_queue, shared_memory_pointer,
                 read_size, max_batches=None, **kwargs):
        """
        :param worker_index:
        :param in_queue:
        :param out_queue:
        :param shared_memory_pointer:
        """

        # TODO not sure if this is the correct syntax
        # https://github.com/numpy/numpy/blob/master/numpy/core/memmap.py
        super(BaseReader, self).__init__(worker_id=worker_id + READER_OFFSET,
                                         **kwargs)
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.shared_memory_pointer = shared_memory_pointer
        self.max_batches = max_batches
        self.read_size = read_size
        self.reader_id = self.worker_id - READER_OFFSET

    def debug(self, message):
        if isinstance(message, list):
            message = " ".join(message)
        # super(BaseReader, self).debug(" :READER #{0}: ".format(self.reader_id) + message)
        reader_logger.debug(" reader_id={0} ".format(self.worker_id) + message)
        # if isinstance(message, list):
        #     message = " ".join(message)
        # lg.info("READER #{0}: {1}".format(self.reader_id, message))

    def info(self, message):
        if isinstance(message, list):
            message = " ".join(message)
        # super(BaseReader, self).debug(" :READER #{0}: ".format(self.reader_id) + message)
        reader_logger.info(
                " reader_id={0} reader_batch_id={1} ".format(self.worker_id,
                                                             self.batch_count) + message)

    def critical(self, message):
        if isinstance(message, list):
            message = " ".join(message)
        reader_logger.critical(
                " reader_id={0} reader_batch_id={1} ".format(self.worker_id,
                                                             self.batch_count) + message)

    def run(self, **kwargs):
        self.read()

    def read(self):
        """

       :return:
        """

        self.info("starting...")
        in_queue_time = time.time()
        # while not self.should_stop() and (
        #                 self.max_batches is None or self.batch_count < self.max_batches):
        # while not self.should_stop() or (
        #                 self.max_batches is not None and self.batch_count >= self.max_batches):
        while not self.should_stop() and (
                        self.max_batches is None or self.batch_count < self.max_batches):

            batch = self.get_batch()
            if batch is not None:
                # self.info("in_queue_get_wait_time={0} in_queue_size={1}".format(time.time() - in_queue_time,
                # self.in_queue.qsize()))
                self.debug("in_queue_get_wait_time={0}".format(
                        time.time() - in_queue_time))
                start_time = time.time()
                self.debug("starting to prepare batch")
                data_pointer = self.process_batch(batch)
                self.debug(
                        "process_batch_time={0}".format(time.time() - start_time))

                if data_pointer is None:
                    self.critical(
                            "process_batch returned None, exiting... {0}".format(
                                    batch))
                    return False

                self.debug("starting out_queue.put")
                wait_time = time.time()
                while not self.should_stop():
                    try:
                        self.out_queue.put(data_pointer, timeout=0)
                        self.debug("successfully put data in out_queue")
                        break
                    except Queue.Full:
                        # self.debug("out_queue is full, sleeping")
                        self.sleep()

                self.debug(
                        "batch_read_time={0} out_queue_put_wait_time={1}".format(
                                time.time() - start_time, time.time() - wait_time))
                # self.info("batch_read_time={0} out_queue_put_wait_time={1} out_queue_size={2}".format(time.time() -
                #  start_time, time.time() - wait_time, self.out_queue.qsize()))

                self.batch_count += 1
                in_queue_time = time.time()
            else:
                self.sleep()
                # time.sleep(self.sleep_duration)
                pass

        self.debug("exiting...")
        self.seppuku()

    def get_batch(self):
        try:
            return self.in_queue.get(timeout=1)
        except Queue.Empty:
            return None

    def process_batch(self, batch, **kwargs):
        return batch

    def find_bucket(self):
        self.debug("attempting to find a bucket")
        start_time = time.time()
        while not self.should_stop():
            for bucket_index, bucket in enumerate(self.shared_memory_pointer):
                with bucket[0].get_lock():
                    if bucket[0].value == 0:
                        bucket[0].value = 1
                        self.debug(
                                "bucket_seek_time={0} bucket_index={1}".format(
                                        time.time() - start_time, bucket_index))
                        return bucket_index
            self.sleep()


class H5Reader(BaseReader):
    def __init__(self, worker_id, in_queue, out_queue, shared_memory_pointer,
                 read_size, class_index_map, file_index_list,
                 max_batches=None,
                 process_function=None,
                 make_class_index=False,
                 make_one_hot=False,
                 make_file_index=False,
                 shuffle=True,
                 cache_handles=False,
                 **kwargs):
        super(self.__class__, self).__init__(worker_id=worker_id,
                                             in_queue=in_queue,
                                             out_queue=out_queue,
                                             shared_memory_pointer=shared_memory_pointer,
                                             read_size=read_size,
                                             **kwargs)
        # necessary data items
        self.max_batches = max_batches
        self.process_function = process_function
        self.class_index_map = class_index_map
        self.file_index_list = file_index_list

        # output switches
        self.make_class_index = make_class_index
        self.make_one_hot = make_one_hot
        self.make_file_index = make_file_index
        self.shuffle = shuffle
        self.cache_handles = cache_handles

    def process_batch(self, batch, store_in_shared=True):
        batch_id = 0
        if store_in_shared:
            bucket_index = self.find_bucket()
        else:
            bucket_index = 0
        data_sets = list()

        # saving memory address of shared_memory_pointer for check later
        # tmp = hex(id(self.shared_memory_pointer[0][0][1][0]))

        # payloads = list()
        payloads = collections.OrderedDict()

        # file_list = list()
        # file_struct = collections.OrderedDict()
        tmp_file_struct = list()

        tmp_index_payload = None
        start = 0
        n_read_requests = len(batch)

        payloads_build_time = time.time()
        for read_index, read_request in enumerate(batch):
            file_path_index, data_sets, class_name, read_descriptor, batch_id = read_request

            file_path = self.file_index_list[file_path_index]

            if self.cache_handles:
                if file_path in self.file_handle_holder:
                    h5_file_handle = self.file_handle_holder[file_path]
                else:
                    h5_file_handle = self.file_handle_holder[file_path] = h5py.File(file_path, 'r')
            else:
                h5_file_handle = h5py.File(file_path, 'r')

            h5_to_payloads_time = time.time()
            for data_set in data_sets:
                # if len(payloads) < data_set_index + 1:
                #     payloads.append(range(n_read_requests))
                # payload = payloads[data_set_index]
                try:
                    payloads[data_set][read_index]
                except KeyError:
                    payloads[data_set] = range(n_read_requests)

                if isinstance(read_descriptor, tuple):
                    # TODO use read_direct function instead
                    # http://docs.h5py.org/en/latest/high/dataset.html
                    payloads[data_set][read_index] = np.array(h5_file_handle[data_set][
                                                              read_descriptor[0]:read_descriptor[
                                                                  1]])
                    # payloads[data_set_index][read_index] = h5_file_handle[data_set][read_descriptor[
                    # 0]:read_descriptor[1]]

                elif isinstance(read_descriptor, list) or isinstance(read_descriptor, np.ndarray):
                    # TODO there is potentially a faster way
                    # https://stackoverflow.com/questions/21766145/h5py-correct-way-to-slice-array-datasets
                    # payloads[data_set][read_index] = np.take(h5_file_handle[data_set], read_descriptor, axis=0)
                    payloads[data_set][read_index] = h5_file_handle[data_set][read_descriptor]

            self.debug("h5_to_payloads_time={0} read_index={1} batch_id={2}".format(time.time() - h5_to_payloads_time,
                                                                                   read_index,
                                                                                   batch_id))

            if tmp_index_payload is None:
                tmp_index_payload = np.zeros(self.read_size)

            if isinstance(read_descriptor, tuple):
                n_examples = read_descriptor[1] - read_descriptor[0]
                for index in range(read_descriptor[0], read_descriptor[1]):
                    tmp_file_struct.append((file_path_index, index))

            elif isinstance(read_descriptor, list) or isinstance(read_descriptor, np.ndarray):
                n_examples = len(read_descriptor)
                for index in read_descriptor:
                    tmp_file_struct.append((file_path_index, index))

            tmp_class_index = np.full(n_examples, self.class_index_map[class_name])
            try:
                tmp_index_payload[start:start + n_examples] = tmp_class_index
            except ValueError as e:
                lg.critical("n_examples={} start={} len(tmp_class_index)={}".format(n_examples,
                                                                                    start,
                                                                                    len(tmp_class_index)))
                raise_from(ValueError, e)

            # tmp_file_index = np.full(n_examples, self.class_index_map[class_name])
            # for thing in range(n_examples):

            start += n_examples

            if not self.cache_handles:
                h5_file_handle.close()

        self.debug("payloads_build_time={0} batch_id={1}".format(
                time.time() - payloads_build_time, batch_id))

        concat_time = time.time()
        for data_set in payloads:
            try:
                payloads[data_set] = np.concatenate(payloads[data_set])
            except ValueError as e:
                print(e)
                print(payloads[data_set])
                raise ValueError(e)

        self.debug(
                "concat_time={0} batch_id={1}".format(time.time() - concat_time,
                                                      batch_id))

        if self.make_class_index:
            payloads['class_index'] = tmp_index_payload

        if self.make_one_hot:
            tmp_one_hot = keras.utils.np_utils.to_categorical(tmp_index_payload,
                                                              len(self.class_index_map))
            payloads['one_hot'] = tmp_one_hot

        if self.make_file_index:
            payloads['file_struct'] = np.array(tmp_file_struct)

        process_time = time.time()
        if self.process_function is not None:
            # print('using process function')
            payloads = self.process_function(payloads)
            self.debug("process_function_time={0} batch_id={1}".format(
                    time.time() - process_time, batch_id))
        else:
            payloads = payloads.values()

        # if self.make_class_index:
        #     payloads.append(tmp_index_payload)

        # if self.make_one_hot:
        #     tmp_one_hot = keras.utils.np_utils.to_categorical(tmp_index_payload,
        #                                                       len(self.class_index_map))
        #     payloads.append(tmp_one_hot)

        if self.shuffle:
            payloads = self.shuffle_in_unison_inplace(payloads)

        if store_in_shared:
            store_in_shared_time = time.time()
            for data_set_index, payload in enumerate(payloads):
                try:
                    self.shared_memory_pointer[bucket_index][1][data_set_index][...] = np.copy(payload)
                except TypeError as e:
                    self.critical(e)
                    self.critical("HELP!> SHARED MEMORY FAILED, bucket_index={} data_set_index={} payload={}".format(bucket_index,
                                                                                                                     data_set_index,
                                                                                                                     payload))

            self.debug("store_in_shared_time={0} batch_id={1}".format(
                    time.time() - store_in_shared_time, batch_id))
            return self.worker_id - READER_OFFSET, bucket_index, data_sets, batch_id
        else:
            return payloads

    def shuffle_in_unison_inplace(self, payloads):
        shuffle_list = np.random.permutation(self.read_size)

        out = []
        for payload in payloads:
            assert self.read_size == len(payload)
            out.append(payload[shuffle_list])

        return out
