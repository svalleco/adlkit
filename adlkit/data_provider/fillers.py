import Queue
import copy
import logging as lg
import time
from collections import OrderedDict

import h5py
from numpy import random

from .config import FILLER_OFFSET
from .workers import Worker

RANDOM = random.random

filler_logger = lg.getLogger('data_provider.workers.fillers')


class BaseFiller(Worker):
    def __init__(self, in_queue, malloc_queue, worker_id, read_batches_per_epoch=None,
                 max_batches=None, **kwargs):
        super(BaseFiller, self).__init__(worker_id + FILLER_OFFSET, **kwargs)

        self.in_queue = in_queue
        self.malloc_queue = malloc_queue

        self.max_batches = max_batches
        self.read_batches_per_epoch = read_batches_per_epoch
        self.data_set_tracker = dict()

    def debug(self, message):
        if isinstance(message, list):
            message = " ".join(message)
        filler_logger.debug(" filler_id={0} ".format(self.worker_id) + message)
        # TODO prettier logging format like the following
        # filler_logger.debug(" filler_id={0:<3d} ".format(self.worker_id) + message)

    def info(self, message):
        if isinstance(message, list):
            message = " ".join(message)
        filler_logger.info(" filler_id={0} filler_batch_id={1} ".format(self.worker_id,
                                                                        self.batch_count) + message)

    def build_batch(self):
        return

    def reset(self):
        return

    def run(self, **kwargs):
        self.fill()

    def fill(self):
        """
        :return:
        """

        self.debug("starting...")

        while not self.should_stop() and (
                        self.max_batches is None or self.batch_count < self.max_batches):

            if self.read_batches_per_epoch is not None and self.batch_count % self.read_batches_per_epoch == 0:
                self.reset()

            start_time = time.time()
            self.debug("start build_batch")
            batch = self.build_batch()
            self.info("build_batch_time={0}".format(time.time() - start_time))

            if batch is None:
                return False

            # put cannot be trusted, must poll manually

            in_queue_put_wait_time = time.time()
            self.debug("start in_queue.put")
            while not self.should_stop():
                try:
                    self.in_queue.put(batch, block=False)
                    self.debug("successfully put data in in_queue")
                    break
                except Queue.Full:
                    self.debug("in_queue is full, sleeping")
                    self.sleep()

            self.info(
                    "batch_fill_time={0} in_queue_put_wait_time={1}".format(time.time() - start_time,
                                                                            time.time() - in_queue_put_wait_time))
            self.batch_count += 1

        self.debug("exiting...")
        self.seppuku()


class H5Filler(BaseFiller):
    """

    """

    # TODO data_sets required
    def __init__(self, classes, class_index_map, in_queue, malloc_queue, worker_id, read_size,
                 data_sets, file_index_list,
                 read_batches_per_epoch=None,
                 max_batches=None,
                 filter_function=None,
                 skip=0,
                 wrap_examples=False,
                 shape_reader=None,
                 **kwargs):
        """

        :param list_of_file_names: list()
            fully specified file_name paths

        :param list_of_data_sets:  list()

        :param batch_size: int()

        :param skip: int()

        :param kwargs:


        """

        super(H5Filler, self).__init__(in_queue=in_queue,
                                       worker_id=worker_id,
                                       max_batches=max_batches,
                                       malloc_queue=malloc_queue,
                                       read_batches_per_epoch=read_batches_per_epoch)
        self.skip = skip
        self.read_size = read_size

        if callable(filter_function):
            self.filter_function = dict()
            for key in classes.keys():
                self.filter_function[key] = filter_function
        elif isinstance(filter_function, dict) or filter_function is None:
            self.filter_function = filter_function
        else:
            raise ValueError

        self.read_size = read_size
        self.classes = classes
        self.original_classes = copy.deepcopy(classes)
        self.class_index_map = class_index_map
        self.wrap_examples = wrap_examples
        self.data_set_tracker = dict((data_set, False) for data_set in data_sets)
        self.shape_reader = shape_reader

        self.filler_id = self.worker_id - FILLER_OFFSET

        self.file_index_list = file_index_list
        self.file_counter = OrderedDict()

        self.report = True

    def inform_data_provider(self, data_sets, batch):
        malloc_requests = list()
        if self.shape_reader is None:
            ruler_data_set = data_sets[0]

            file_name = self.file_index_list[batch[0][0]]
            if file_name in self.file_handle_holder:
                h5_file_handle = self.file_handle_holder[file_name]
            else:
                h5_file_handle = self.file_handle_holder[file_name] = h5py.File(file_name, 'r')

            shape = h5_file_handle[ruler_data_set][0].shape

            for data_set in data_sets:
                malloc_requests.append((data_set, shape))

        else:
            payloads = self.shape_reader.process_batch(batch, store_in_shared=False)
            for item_index, item in enumerate(payloads):
                shape = item.shape[1:]
                name = 'inferred_{}'.format(item_index)
                malloc_requests.append((name, shape))

        while True:
            try:
                self.malloc_queue.put(malloc_requests)
                break
            except Queue.Full:
                pass
        self.debug("informing data provider of malloc shapes `{0}`".format(malloc_requests))
        self.report = False

    def compute_probability(self):
        """
        # Here we determine how many of each class we want for this batch
        :return:
        """
        use_class = str()
        for _ in range(self.read_size):
            my_number = RANDOM()
            running_sum = 0
            for class_name in self.classes:
                if (running_sum + self.classes[class_name]['class_prob']) > my_number > running_sum:
                    use_class = class_name
                    break
                else:
                    running_sum += self.classes[class_name]['class_prob']

            self.classes[use_class]['n_examples'] += 1

        for class_name in self.classes:
            self.debug(
                    "batch_id={0} n_examples={1} class_name={2}".format(self.batch_count,
                                                                        self.classes[class_name][
                                                                            'n_examples'], class_name))

    def build_batch(self):
        """
        this mechanism assumes that each "dataset" has an equal amount of data points, else AssertionError
        :return:
        """

        batch = list()
        tmp_data_set_tracker = list()

        self.compute_probability()

        for class_name in self.classes:

            tmp_class_holder = self.classes[class_name]

            tmp_examples_added = 0
            while tmp_examples_added < tmp_class_holder['n_examples']:
                if tmp_class_holder['example_index'] == 0:
                    file_name = tmp_class_holder['file_names'][tmp_class_holder['file_index']]
                    file_name = self.file_index_list[file_name]

                    self.debug(['Opening:', class_name, str(tmp_class_holder['file_index']),
                                file_name])

                    # TODO make a switch that can either use `with` or self.file_handle_holder
                    if file_name in self.file_handle_holder:
                        h5_file_handle = self.file_handle_holder[file_name]
                    else:
                        h5_file_handle = self.file_handle_holder[file_name] = h5py.File(file_name,
                                                                                        'r')

                    for data_set in tmp_class_holder['data_set_names']:
                        if data_set not in tmp_data_set_tracker:
                            tmp_data_set_tracker.append(data_set)

                    # TODO implement the assert
                    # assert (sum(tmp_list) != len(tmp_list) * tmp_list[0],
                    #         "{0} has datasets with mismatched tensor shapes".format(file_name))

                    filter_time = time.time()
                    # TODO convert to calling function from dictionary by class_name
                    if self.filter_function is not None:
                        tmp_filter_index_list = self.filter_function[class_name](h5_file_handle,
                                                                                 tmp_class_holder[
                                                                                     "data_set_names"])
                    else:
                        tmp_filter_index_list = range(
                                h5_file_handle[tmp_class_holder["data_set_names"][0]].shape[0])

                    self.info("filter_function_time={0}".format(time.time() - filter_time))

                    tmp_class_holder['current_file_indices'] = sorted(tmp_filter_index_list)

                start_index = tmp_class_holder['example_index']

                possible_end_index = tmp_class_holder['example_index'] \
                                     + tmp_class_holder['n_examples'] \
                                     - tmp_examples_added

                max_index_possible = len(tmp_class_holder['current_file_indices']) - 1

                end_index = min(possible_end_index, max_index_possible)

                if self.filter_function is not None:
                    read_descriptor = tmp_class_holder['current_file_indices'][
                                      start_index:end_index]
                else:
                    read_descriptor = (tmp_class_holder['current_file_indices'][start_index],
                                       tmp_class_holder['current_file_indices'][end_index])

                # TODO figure out how to test this better
                if start_index == end_index:
                    read_descriptor = [tmp_class_holder['current_file_indices'][start_index]]
                    tmp_examples_added += 1
                else:
                    tmp_examples_added += end_index - start_index

                batch.append((tmp_class_holder['file_names'][tmp_class_holder['file_index']],
                              tmp_class_holder['data_set_names'],
                              class_name,
                              read_descriptor,
                              self.batch_count))

                if end_index >= max_index_possible or start_index == end_index:

                    tmp_class_holder['example_index'] = 0
                    tmp_class_holder['file_index'] += 1

                    # TODO refine wrap logic
                    if len(tmp_class_holder['file_names']) == tmp_class_holder['file_index']:
                        if self.wrap_examples:
                            tmp_class_holder['file_index'] = 0
                        else:
                            return None

                else:
                    tmp_class_holder['example_index'] = end_index

        for class_name in self.classes:
            self.classes[class_name]['n_examples'] = 0

        if self.report:
            self.inform_data_provider(tmp_data_set_tracker, batch)

        if isinstance(batch[0][3], list):
            batch_sum_check = sum(len(item[3]) for item in batch)
        elif isinstance(batch[0][3], tuple):
            batch_sum_check = sum(item[3][1] - item[3][0] for item in batch)
        else:
            # guaranteed fail case
            batch_sum_check = 0
        assert self.read_size == batch_sum_check
        return batch

    def reset(self):
        self.classes = copy.deepcopy(self.original_classes)
