import Queue
import ctypes
import gc
import logging as lg
import multiprocessing
import signal
import time
from abc import ABCMeta, abstractmethod

import numpy as np

from .config import ConfigurableObject, STOP_MESSAGE
from .fillers import BaseFiller, H5Filler
from .generators import BaseGenerator
from .readers import BaseReader, H5Reader
from .watchers import BaseWatcher

data_provider_logger = lg.getLogger('data_provider.main.dataprovdr')


class BaseDataProvider(ConfigurableObject):
    name = None
    error = None
    __metaclass__ = ABCMeta

    def __init__(self, default_config, config=None, name=None):
        super(BaseDataProvider, self).__init__(default_config, config)
        self.name = name or 'DataProvider'

    @abstractmethod
    def start(self, **kwargs):
        pass

    @abstractmethod
    def hard_stop(self, **kwargs):
        pass

    @abstractmethod
    def generate(self):
        """
        This method shall either return a single batch or None
        :return:
        """
        pass


class FileDataProvider(BaseDataProvider):
    def __init__(self, sample_specification, **kwargs):
        """
        
        :param kwargs: 
        """
        default_config = {
            'batch_size': 2048,

            # If initial indices should be skipped, set this to the
            # correspinding index.
            'skip': 0,

            # The max number of batches to deliver, per generator.
            'max': 1e12,

            # If examples can be reused.
            'wrap_examples': True,

            # The number of reader processes to spawn. Each takes about ~4
            # file descriptors, so if you run out => make this number smaller.
            'n_readers': 20,

            # How many generators will be reading the same data? Less than
            # one will cause an error. -1 might work...
            'n_generators': 1,

            # If it is more efficient to read multiple batches at a time,
            # increment this.
            'read_multiplier': 1,

            # Not Implemented
            'waittime': 0.005,

            # False Not Implemented
            'use_shared_memory': True,

            # How many buckets should each reader have? When tuning,
            # scale with read_multiplier.
            'n_buckets': 10,

            # Queue depth coefficient for scaling with number of readers.
            'q_multipler': 1,

            # Not Implemented
            'GeneratorTimeout': 10,

            # Not Implemented
            'SharedDataQueueSize': 1,

            # 'timing': False,

            # A filter_function will be given the first index of the
            # sample_specification and asked to produce a list of read_indices.
            # It can either be a single function or a dictionary of class_name:function pairs.
            'filter_function': None,

            # TODO convert to {'fun_name': function()}
            # A process_function will consume an OrderedDictionary and be
            # expected to produce a list of numpy arrays to store into the
            # shared memory.
            'process_function': None,

            # If you need to swap the order, downsample, or any
            # other last-minute operation, do that here. Given a list of
            # numpy tensors, return a list of numpy tensors.
            # **NOTE** this is done in the h5_file_insert process.
            'delivery_function': None,

            # If the batch should contain the class index tensor and the
            # one_hot tensors.
            'make_class_index': False,
            'make_one_hot': False,

            # If the batch should contain a reference to the file+index combo that the data point
            # came from.
            'make_file_index': False,

            # Not implemented.
            'catch_signals': False,

            # This is a race condition catch. Probably not a good idea to
            # disable but may make sense in edge-cases.
            'wait_for_malloc': True,

            # The amount of time a worker should sleep if they are blocked by resource constraints.
            # This is used with os.sleep so either a float or an int should work.
            'sleep_duration': 1,

            # If the rows should be shuffled on a per-batch basis.
            'shuffle': True,

            # If the reader should cache the file handles or close files after reading.
            'cache_handles': False,
        }

        # TODO decompose defaults into super classes
        super(FileDataProvider, self).__init__(default_config, kwargs)

        #############
        # for key, value in kwargs.items():
        #     setattr(self, key, value)
        #############

        self.process_sample_specification(sample_specification)

        self.config.read_size = self.config.batch_size * self.config.read_multiplier

        # self.catchsignals = catchsignals

        self.filler_count = 0
        self.reader_count = 0
        self.generator_count = 0

        self.in_queue = None
        self.out_queue = None
        self.malloc_queue = None

        self.multicast_queues = list()

        self.stop_check = False
        self.is_started = False

        self.filler = None
        # self.fillers = list()
        self.readers = list()
        self.watcher = None
        self.generators = list()

        # self.preloaded = False

        self.shared_memory = list()
        self.malloc_requests = list()
        self.extra_malloc_requests = list()

        if self.config.make_class_index:
            self.extra_malloc_requests.append(
                ('class_index', tuple())
            )

        if self.config.make_one_hot:
            self.extra_malloc_requests.append(
                ('one_hot', (len(self.config.classes),))
            )

        self.config.translate_col_to_file_name = None
        if self.config.make_file_index:
            self.extra_malloc_requests.append(
                ('file_index', (2,))
            )
            self.config.translate_col_to_file_name = -1

        # TODO signal catching
        self.sig1 = signal.getsignal(signal.SIGINT)
        self.sig2 = signal.getsignal(signal.SIGTERM)

    def should_stop(self):
        return self.stop_check

    def process_sample_specification(self, sample_specification):
        self.config.sample_specification = sample_specification
        self.config.classes, self.config.class_index_map, self.config.data_sets, self.config.file_index_list = self.build_classes_from_files(
            sample_specification)

    @staticmethod
    def build_classes_from_files(sample_specification, class_index_map=None):
        classes = {}
        class_count = 0

        tmp_class_index_map = dict()
        data_sets = dict()
        tmp_file_index_list = list()

        for sample in sample_specification:

            if len(sample) == 4:
                # TODO there should be a mechanism to validate this
                file_name = sample[0]
                data_set_names = sample[1]
                class_name = sample[2]
                class_prob = sample[3]

            else:
                raise IndexError('!!bad sample_specification!!')

            for data_set in data_set_names:
                abridged_data_set = data_set.split('/')[0]
                data_sets[abridged_data_set] = True

            if isinstance(class_index_map, dict):
                class_index = class_index_map[class_name]
            else:
                try:
                    class_index = tmp_class_index_map[class_name]
                except KeyError:
                    tmp_class_index_map[class_name] = class_index = class_count
                    class_count += 1

            tmp_file_index_list.append(file_name)
            file_index = len(tmp_file_index_list) - 1

            try:
                classes[class_name]['file_names'].append(file_index)
            except KeyError:
                classes[class_name] = {
                    "file_names": [file_index],
                    "n_examples": 0,
                    "file_index": 0,
                    "example_index": 0,
                    "data_set_names": data_set_names,
                    "class_index": class_index,
                    "file_handle": False,
                    "class_prob": class_prob,
                    "current_file_indices": None
                }

        if isinstance(class_index_map, dict):
            tmp_class_index_map = class_index_map

        tmp = list()
        for class_name in classes:
            tmp.append(classes[class_name]['class_prob'])

        sum_tmp = float(sum(tmp))

        for class_name in classes:
            classes[class_name]['class_prob'] = classes[class_name][
                                                    'class_prob'] / sum_tmp

        return classes, tmp_class_index_map, data_sets, tmp_file_index_list

    def debug(self, message):
        if isinstance(message, list):
            message = " ".join(message)
        data_provider_logger.debug(" dtprvd_id=1 " + message)
        # TODO prettier logging format like the following
        # filler_logger.debug(" filler_id={0:<3d} ".format(self.worker_id) + message)

    def info(self, message):
        if isinstance(message, list):
            message = " ".join(message)
        data_provider_logger.info(" dtprvd_id=1 " + message)

    def process_malloc_requests(self):
        malloc_wait_time = time.time()

        malloc_requests = self.malloc_queue.get()

        # for index, request in self.malloc_requests:
        #     if request[0] == malloc_request[0]:
        #         self.malloc_requests[index] = malloc_request
        #         malloc_request = None

        if malloc_requests is not None:
            self.malloc_requests.extend(malloc_requests)
        self.info("malloc_wait_time={0}".format(time.time() - malloc_wait_time))

    def make_shared_malloc(self, in_reader_id):
        """
        This should be executed when a new reader is added.
        :return:
        """

        loop_list = []

        if isinstance(in_reader_id, int):
            loop_list.append(in_reader_id)

        elif isinstance(in_reader_id, list):
            loop_list = in_reader_id

        for reader_id in loop_list:
            # ensuring we don't index error
            if len(self.shared_memory) < reader_id + 1:
                self.shared_memory.extend(
                    range(len(self.shared_memory), reader_id + 1))

            buckets = list()
            for bucket in range(self.config.n_buckets):
                data_sets = []
                for request in (
                            self.malloc_requests + self.extra_malloc_requests):
                    # reshape the requested shape to match the read_size
                    shape = (self.config.read_size,) + request[1]

                    shared_array_base = multiprocessing.Array(ctypes.c_double,
                                                              np.prod(shape),
                                                              lock=False)
                    shared_array = np.ctypeslib.as_array(shared_array_base)
                    shared_array = shared_array.reshape(shape)
                    data_sets.append(shared_array)

                state = multiprocessing.Value('i', 0)
                generator_start_counter = multiprocessing.Value('i', 0)
                generator_end_counter = multiprocessing.Value('i', 0)
                buckets.append([state, data_sets, generator_start_counter,
                                generator_end_counter])

                self.shared_memory[reader_id] = buckets

    # def wait_for_malloc_requests(self):
    #     malloc_wait_time = time.time()
    #     # TODO better logic is needed to determine when to hard_stop waiting, data set naming is weird
    #     while len(self.config.data_sets) > len(self.malloc_requests) and not self.should_stop():
    #         self.process_malloc_requests(timeout=1)


    def start(self, filler_class, reader_class, generator_class,
              watcher_class=None, shape_reader_class=None, **kwargs):
        """

        :param filler_class:
        :param reader_class:
        :param generator_class:
        :param watcher_class:
        :param shape_reader_class:
        :param kwargs: sent to all fillers / readers
        :return:
        """
        start_time = time.time()
        # TODO check instance

        # TODO start x2 (reset)
        if self.is_started is True:
            self.debug("already started!")
            return
        self.start_queues()

        self.start_filler(filler_class, shape_reader_class=shape_reader_class,
                          **kwargs)
        # if self.config.wait_for_malloc:
        #     self.wait_for_malloc_requests()
        self.process_malloc_requests()
        self.make_shared_malloc(range(self.config.n_readers))

        try:
            for reader_id in range(self.config.n_readers):
                self.start_reader(reader_class, **kwargs)
        except Exception as e:
            data_provider_logger.error(e)
            self.hard_stop()
            raise ValueError("n_readers most likely isn't set correctly")

        if watcher_class is not None:
            self.start_watcher(watcher_class)

        try:
            for generator_id in range(self.config.n_generators):
                self.start_generator(generator_class, **kwargs)
        except Exception as e:
            data_provider_logger.error(e)
            self.hard_stop()
            raise ValueError("n_generators most likely isn't set correctly")

        self.is_started = True
        self.info("start_time={0}".format(time.time() - start_time))

    def start_filler(self, filler_class, shape_reader_class=None, **kwargs):
        shape_reader = None
        if shape_reader_class is not None and self.config.process_function is not None:
            if not issubclass(shape_reader_class, BaseReader):
                message = "cannot use reader of type {0} or process function was not given".format(
                    type(shape_reader_class))
                raise ValueError(message)

            shape_reader = shape_reader_class(worker_id=0,
                                              in_queue=None,
                                              out_queue=None,
                                              shared_memory_pointer=list(),
                                              max_batches=0,
                                              read_size=self.config.read_size,
                                              class_index_map=self.config.class_index_map,
                                              file_index_list=self.config.file_index_list,
                                              process_function=self.config.process_function)

        # TODO handle case where filler already exists
        if issubclass(filler_class, BaseFiller):
            filler_id = self.filler_count
            self.filler_count += 1

            self.filler = filler_class(classes=self.config.classes,
                                       class_index_map=self.config.class_index_map,
                                       file_index_list=self.config.file_index_list,
                                       in_queue=self.in_queue,
                                       worker_id=filler_id,
                                       read_size=self.config.read_size,
                                       malloc_queue=self.malloc_queue,
                                       data_sets=self.config.data_sets,
                                       shape_reader=shape_reader,
                                       wrap_examples=self.config.wrap_examples,
                                       filter_function=self.config.filter_function,
                                       sleep_duration=self.config.sleep_duration,
                                       **kwargs)

            self.filler.daemon = True
            self.filler.start()
            return filler_id
        else:
            return None

    def start_reader(self, reader_class, **kwargs):
        if issubclass(reader_class, BaseReader):
            # TODO possible off by one error
            reader_id = self.reader_count
            self.reader_count += 1

            self.make_shared_malloc(reader_id)

            # extending our arrays to track the readers
            self._extend_array(self.readers, reader_id)

            self.readers[reader_id] = reader_class(in_queue=self.in_queue,
                                                   out_queue=self.out_queue,
                                                   shared_memory_pointer=
                                                   self.shared_memory[reader_id],
                                                   worker_id=reader_id,
                                                   read_size=self.config.read_size,
                                                   class_index_map=self.config.class_index_map,
                                                   file_index_list=self.config.file_index_list,
                                                   make_one_hot=self.config.make_one_hot,
                                                   make_class_index=self.config.make_class_index,
                                                   make_file_index=self.config.make_file_index,
                                                   process_function=self.config.process_function,
                                                   sleep_duration=self.config.sleep_duration,
                                                   shuffle=self.config.shuffle,
                                                   cache_handles=self.config.cache_handles,
                                                   **kwargs)

            self.readers[reader_id].daemon = True
            self.readers[reader_id].start()

            return reader_id
        else:
            return None

    def start_generator(self, generator_class, **kwargs):
        if issubclass(generator_class, BaseGenerator):
            generator_id = self.generator_count
            self.generator_count += 1

            self._extend_array(self.generators, generator_id)

            if self.watcher is None:
                out_queue = self.out_queue
                watched = False
            else:
                out_queue = self.multicast_queues[generator_id]
                watched = True

            # out_queue,
            # batch_size,
            # shared_memory_pointer,
            # file_index_list,

            self.generators[generator_id] = generator_class(
                out_queue=out_queue,
                batch_size=self.config.batch_size,
                shared_memory_pointer=self.shared_memory,
                file_index_list=self.config.file_index_list,
                worker_id=generator_id,
                watched=watched,
                translate_col_to_file_name=self.config.translate_col_to_file_name,
                delivery_function=self.config.delivery_function,
                sleep_duration=self.config.sleep_duration,
                **kwargs)

            return generator_id
        return None

    def start_watcher(self, watcher_class, **kwargs):
        if issubclass(watcher_class, BaseWatcher):
            watcher_id = 0
            self.watcher = watcher_class(worker_id=watcher_id,
                                         shared_memory_pointer=self.shared_memory,
                                         out_queue=self.out_queue,
                                         multicast_queues=self.multicast_queues,
                                         sleep_duration=self.config.sleep_duration,
                                         **kwargs)

            self.watcher.daemon = True
            self.watcher.start()

            return watcher_id
        else:
            return None

    def start_queues(self):
        self.in_queue = multiprocessing.Queue(
            maxsize=self.config.q_multipler * self.config.n_readers)
        self.out_queue = multiprocessing.Queue(
            maxsize=self.config.q_multipler * self.config.n_readers)
        self.malloc_queue = multiprocessing.Queue(
            maxsize=self.config.q_multipler * self.config.n_readers)

        for _ in range(self.config.n_generators):
            self.multicast_queues.append(
                # multiprocessing.Queue(maxsize=self.config.q_multipler * self.config.n_generators))
                multiprocessing.Queue(maxsize=self.config.q_multipler * self.config.n_readers))

    def stop_queues(self):
        try:
            self.in_queue.close()
            self.in_queue = None
        except Exception as e:
            data_provider_logger.error(e)

        try:
            self.out_queue.close()
            self.out_queue = None
        except Exception as e:
            data_provider_logger.error(e)

        try:
            self.malloc_queue.close()
            self.malloc_queue = None
        except Exception as e:
            data_provider_logger.error(e)

        for queue_index in range(len(self.multicast_queues)):
            try:
                self.multicast_queues[queue_index].close()
            except Exception as e:
                data_provider_logger.error(e)

        self.multicast_queues = list()

    def hard_stop(self):
        try:
            self.stop_filler()
        except Exception as e:
            print(e)

        for reader_index in range(len(self.readers)):
            try:
                self.stop_reader(reader_index)
            except Exception as e:
                data_provider_logger.debug(e)

        # if self.watcher:
        #     self.watcher.s

        self.drain_queues()
        self.stop_queues()

        try:
            self.filler.join()
        except Exception as e:
            data_provider_logger.debug(e)

        for reader in self.readers:
            try:
                reader.join()
            except Exception as e:
                data_provider_logger.debug(e)

        if self.watcher is not None:
            try:
                self.watcher.join()
            except Exception as e:
                data_provider_logger.debug(e)

        # attempting to free memory, according to the internet, this may or may not actually work.
        del self.shared_memory
        gc.collect()

        self.is_started = False

    def drain_queues(self):
        while True:
            try:
                self.in_queue.get(timeout=1)
            except Queue.Empty:
                break

        while True:
            try:
                self.out_queue.get(timeout=1)
            except Queue.Empty:
                break

        while True:
            try:
                self.malloc_queue.get(timeout=1)
            except Queue.Empty:
                break

        for queue_index in range(len(self.multicast_queues)):
            while True:
                try:
                    self.multicast_queues[queue_index].get(timeout=1)
                except Queue.Empty:
                    break
                break

    def stop_filler(self):
        """

        if stop fails, kill by pid / handle at end of timeout ?
        :return:
        """
        self.filler.send_command(STOP_MESSAGE)

    def stop_reader(self, reader_id):
        try:
            self.readers[reader_id].send_command(STOP_MESSAGE)
        except IndexError:
            pass

    def first(self):
        if isinstance(self.generators[0], BaseGenerator):
            return self.generators[0]
        else:
            return None

    def generate(self):
        # TODO
        # Not sure if this makes sense.
        try:
            return self.first().generate()
        # TODO
        # determine what exception this will raise.
        except Exception:
            raise StopIteration

    @staticmethod
    def _extend_array(in_array, in_id):
        if len(in_array) < in_id + 1:
            in_array.extend(range(len(in_array), in_id + 1))


class H5FileDataProvider(FileDataProvider):
    def start(self, **kwargs):
        # super(H5FileDataProvider, self).start(H5Filler, H5Reader, BaseGenerator, watcher_class=BaseWatcher, shape_reader_class=H5Reader, **kwargs)
        super(H5FileDataProvider, self).start(H5Filler, H5Reader, BaseGenerator,
                                              shape_reader_class=H5Reader,
                                              **kwargs)
