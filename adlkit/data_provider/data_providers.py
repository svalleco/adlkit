# -*- coding: utf-8 -*-
"""
ADLKit
Copyright ©2017 AnomalousDL, Inc.  All rights reserved.

AnomalousDL, Inc. (ADL) licenses this file to you under the Academic and Research End User License Agreement (the
"License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at

  http://www.anomalousdl.com/licenses/ACADEMIC-LICENSE.txt

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL ADL BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE, either express
or implied.  See the License for the specific language governing permissions and limitations under the License.
"""

import collections
import ctypes
import logging as lg
import multiprocessing
import signal
import time
from abc import ABCMeta, abstractmethod

import numpy as np
from future.utils import raise_with_traceback

from adlkit.data_provider.comm_drivers import QueueCommDriver
from adlkit.data_provider.io_drivers import IOController
from adlkit.data_provider.writers import BaseWriter
from .config import ConfigurableObject
from .fillers import BaseFiller, FileFiller
from .generators import BaseGenerator
from .readers import BaseReader, FileReader
from .watchers import BaseWatcher
from .workers import EXIT, PRUNE

data_provider_logger = lg.getLogger('data_provider.main.dataprovdr')


class DataProviderError(Exception):
    pass


class AbstractDataProvider(ConfigurableObject):
    name = None
    error = None
    __metaclass__ = ABCMeta

    def __init__(self, default_config, config=None, name=None):
        super(AbstractDataProvider, self).__init__(default_config, config)
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


class FileDataProvider(AbstractDataProvider):
    def __init__(self, sample_specification, **kwargs):
        """
        
        :param kwargs: 
        """
        default_config = {
            'batch_size'            : 2048,

            # If initial indices should be skipped, set this to the
            # correspinding index.
            'skip'                  : 0,

            # The max number of batches to deliver, per generator.
            'max_batches'           : None,

            # If examples can be reused.
            'wrap_examples'         : True,

            # The number of reader processes to spawn. Each takes about ~4
            # file descriptors, so if you run out => make this number smaller.
            'n_readers'             : 20,

            # How many generators will be reading the same data? Less than
            # one will cause an error. -1 might work...
            'n_generators'          : 1,

            # If it is more efficient to read multiple batches at a time,
            # increment this.
            'read_multiplier'       : 1,

            # Not Implemented
            'waittime'              : 0.005,

            # False Not Implemented
            'use_shared_memory'     : True,

            # How many buckets should each reader have? When tuning,
            # scale with read_multiplier.
            'n_buckets'             : 10,

            # Queue depth coefficient for scaling with number of readers.
            'q_multiplier'          : 1,

            # Not Implemented
            'GeneratorTimeout'      : 10,

            # Not Implemented
            'SharedDataQueueSize'   : 1,

            # 'timing': False,

            # A filter_function will be given the first index of the
            # sample_specification and asked to produce a list of read_indices.
            # It can either be a single function or a dictionary of class_name:function pairs.
            'filter_function'       : None,

            # TODO convert to {'fun_name': function()}
            # A process_function will consume an OrderedDictionary and be
            # expected to produce a list of numpy arrays to store into the
            # shared memory.
            'process_function'      : None,

            # If you need to swap the order, downsample, or any
            # other last-minute operation, do that here. Given a list of
            # numpy tensors, return a list of numpy tensors.
            # **NOTE** this is done in the h5_file_insert process.
            'delivery_function'     : None,

            # If the batch should contain the class index tensor and the
            # one_hot tensors.
            'make_class_index'      : False,
            'make_one_hot'          : False,

            # If the batch should contain a reference to the file+index combo that the data point
            # came from.
            'make_file_index'       : False,

            # Whether or not signal handling should be controlled by the DataProvider.
            'catch_signals'         : True,

            # This is a race condition catch. Probably not a good idea to
            # disable but may make sense in edge-cases.
            'wait_for_malloc'       : True,

            # The amount of time a worker should sleep if they are blocked by resource constraints.
            # This is used with os.sleep so either a float or an int should work.
            'sleep_duration'        : 0.5,

            # If the rows should be shuffled on a per-batch basis.
            'shuffle'               : True,

            # If the workers should cache the file handles or close files after reading.
            # NOTE: File handle caching can cause undesired memory allocation.
            # NOTE: The implementation for these were moved to the IOControllers.
            'cache_reader_handles'  : True,
            'cache_filler_handles'  : False,

            # The number of batches a filler should create before resetting.
            'read_batches_per_epoch': None,

            # If you want to set the class_index_map explicitly, this is a mechanism for it.
            'class_index_map'       : None,

            # If it is costly to open connections with your io_ctlr and you don't actually need the handle to work,
            # flipping this switch can improve performance.
            'suppress_opens'        : False,

            # If using the Writer(s), set a config like the following.
            'writer_config'         : None
            # 'writer_config'         : [{
            #     # If using the Writer, the destination url for io_ctlr, else one will be generated.
            #     'data_dst'      : None,
            #
            #     # If using the Writer, the function used to compress / encode the data from the generator.
            #     'pre_write_function': None,
            #     'io_ctlr'     : IOController()
            # }]

        }

        # TODO decompose defaults into super classes
        super(FileDataProvider, self).__init__(default_config, kwargs)

        #######################################
        # for key, value in kwargs.items():
        #     setattr(self, key, value)
        #######################################

        self.process_sample_specification(sample_specification, self.config.get('class_index_map'))

        self.config.read_size = self.config.batch_size * self.config.read_multiplier

        if self.config.catch_signals:
            signal.signal(signal.SIGTERM, self.hard_stop)
            signal.signal(signal.SIGUSR1, self.check_errors_and_stop)

        self.worker_counter = collections.Counter()

        self.comm_driver = None
        self.proxy_comm_drivers = list()

        self.stop_check = False
        self.is_started = False

        self.filler = None
        # self.fillers = list()
        self.readers = list()
        self.watcher = None
        self.generators = list()
        self.writers = list()

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

    def should_stop(self):
        return self.stop_check

    def process_sample_specification(self, sample_specification, class_index_map=None):
        self.config.sample_specification = sample_specification
        self.config.classes, self.config.class_index_map, self.config.data_sets, self.config.file_index_list = \
            self.build_classes_from_files(sample_specification, class_index_map)

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
                    "file_names"          : [file_index],
                    "n_examples"          : 0,
                    "file_index"          : 0,
                    "example_index"       : 0,
                    "data_set_names"      : data_set_names,
                    "class_index"         : class_index,
                    "file_handle"         : False,
                    "class_prob"          : class_prob,
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
        malloc_requests = self.comm_driver.read('malloc')

        if malloc_requests is not None:
            self.malloc_requests.extend(malloc_requests)
        self.debug("malloc_wait_time={0}".format(time.time() - malloc_wait_time))

    def make_shared_malloc(self):
        """
        This should be executed when a new reader is added.
        :return:
        """

        if self.shared_memory is None or (isinstance(self.shared_memory, list) and len(self.shared_memory) == 0):
            self.shared_memory = list()
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
                self.shared_memory.append([state, data_sets, generator_start_counter,
                                           generator_end_counter])

    def start(self, filler_class, reader_class, generator_class,
              filler_io_ctlr=None,
              reader_io_ctlr=None,
              # io_ctlr=None,
              watcher_class=None,
              shape_reader_class=None,
              writer_class=None,
              **kwargs):
        """

        :param filler_class:
        :param reader_class:
        :param generator_class:
        # :param filler_io_ctlr:
        # :param reader_io_ctlr:
        :param io_ctlr:
        :param watcher_class:
        :param shape_reader_class:
        :param writer_class:
        :param kwargs: sent to all fillers / readers
        :return:
        """
        start_time = time.time()
        # TODO check instance

        # TODO start x2 (reset)
        if self.is_started is True:
            self.debug("already started!")
            return False

        if isinstance(filler_io_ctlr, IOController):
            pass
        elif filler_io_ctlr is None:
            filler_io_ctlr = IOController(cache_handles=self.config.cache_filler_handles)
        elif not isinstance(filler_io_ctlr, IOController) and issubclass(filler_io_ctlr, IOController):
            filler_io_ctlr = filler_io_ctlr()
        else:
            if not issubclass(filler_io_ctlr, IOController) or not issubclass(filler_io_ctlr, IOController):
                self.error("bad filler_io_ctlr given, defaulting to H5")
            filler_io_ctlr = IOController(cache_handles=self.config.cache_filler_handles)

        if isinstance(reader_io_ctlr, IOController):
            pass
        elif reader_io_ctlr is None:
            reader_io_ctlr = IOController(cache_handles=self.config.cache_reader_handles)
        elif not isinstance(reader_io_ctlr, IOController) and issubclass(reader_io_ctlr, IOController):
            reader_io_ctlr = reader_io_ctlr()
        else:
            if not isinstance(reader_io_ctlr, IOController) or not issubclass(reader_io_ctlr, IOController):
                self.error("bad reader_io_ctlr given, defaulting to H5")
            reader_io_ctlr = IOController(cache_handles=self.config.cache_reader_handles)

        self.start_queues()

        self.start_filler(filler_class,
                          shape_reader_class=shape_reader_class,
                          shape_reader_io_ctlr=reader_io_ctlr,
                          io_ctlr=filler_io_ctlr,
                          **kwargs)

        self.process_malloc_requests()
        self.make_shared_malloc()

        try:
            for reader_id in range(self.config.n_readers):
                self.start_reader(reader_class,
                                  io_ctlr=reader_io_ctlr,
                                  **kwargs)
        except Exception as e:
            data_provider_logger.error(e)
            self.hard_stop()
            raise_with_traceback(e)

        if watcher_class is not None:
            self.start_watcher(watcher_class)

        try:
            for generator_id in range(self.config.n_generators + len(self.config.writer_config or [])):
                self.start_generator(generator_class, **kwargs)
        except Exception as e:
            data_provider_logger.error(e)
            self.hard_stop()
            raise_with_traceback(e)

        generator_offset = self.config.n_generators
        if writer_class is not None:
            for index in range(len(self.config.writer_config)):
                self.start_writer(writer_class, generator_offset + index)

        self.is_started = True
        self.debug("start_time={0}".format(time.time() - start_time))
        return True

    def start_filler(self, filler_class, io_ctlr, shape_reader_io_ctlr=None, shape_reader_class=None, **kwargs):
        shape_reader = None
        if shape_reader_class is not None and self.config.process_function is not None:
            if not issubclass(shape_reader_class, BaseReader):
                msg = "cannot use reader of type {0} or process function was not given".format(
                        type(shape_reader_class))
                raise ValueError(msg)

            assert shape_reader_io_ctlr is not None
            shape_reader = shape_reader_class(worker_id=0,
                                              comm_driver=self.comm_driver,
                                              shared_memory_pointer=list(),
                                              max_batches=0,
                                              read_size=self.config.read_size,
                                              class_index_map=self.config.class_index_map,
                                              file_index_list=self.config.file_index_list,
                                              process_function=self.config.process_function,
                                              io_ctlr=shape_reader_io_ctlr)

        # TODO handle case where filler already exists
        if issubclass(filler_class, BaseFiller):
            filler_id = self.worker_counter['filler']
            self.worker_counter['filler'] += 1

            self.filler = filler_class(classes=self.config.classes,
                                       class_index_map=self.config.class_index_map,
                                       max_batches=self.config.max_batches,
                                       file_index_list=self.config.file_index_list,
                                       comm_driver=self.comm_driver,
                                       worker_id=filler_id,
                                       read_size=self.config.read_size,
                                       data_sets=self.config.data_sets,
                                       shape_reader=shape_reader,
                                       wrap_examples=self.config.wrap_examples,
                                       filter_function=self.config.filter_function,
                                       sleep_duration=self.config.sleep_duration,
                                       read_batches_per_epoch=self.config.read_batches_per_epoch,
                                       io_ctlr=io_ctlr,
                                       suppress_opens=self.config.suppress_opens,
                                       **kwargs)

            self.filler.daemon = True
            self.filler.start()
            return filler_id
        else:
            return None

    def start_reader(self, reader_class, io_ctlr, **kwargs):
        if issubclass(reader_class, BaseReader):
            reader_id = self.worker_counter['reader']
            self.worker_counter['reader'] += 1

            self.make_shared_malloc()
            self._extend_array(self.readers, reader_id)

            self.readers[reader_id] = reader_class(
                    comm_driver=self.comm_driver,
                    shared_memory_pointer=self.shared_memory,
                    max_batches=self.config.max_batches,
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
                    io_ctlr=io_ctlr,
                    **kwargs)

            self.readers[reader_id].daemon = True
            self.readers[reader_id].start()

            return reader_id
        else:
            return None

    def start_generator(self, generator_class, **kwargs):
        if issubclass(generator_class, BaseGenerator):
            generator_id = self.worker_counter['generator']
            self.worker_counter['generator'] += 1

            self._extend_array(self.generators, generator_id)

            if self.watcher is None:
                comm_driver = self.comm_driver
                watched = False
            else:
                comm_driver = self.proxy_comm_drivers[generator_id]
                watched = True

            self.generators[generator_id] = generator_class(
                    comm_driver=comm_driver,
                    batch_size=self.config.batch_size,
                    max_batches=self.config.max_batches,
                    shared_memory_pointer=self.shared_memory,
                    class_index_map=self.config.class_index_map,
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
            # NOTE - wghilliard - having more than one of these makes sense in edge cases, experimental.
            watcher_id = self.worker_counter['watcher']
            self.worker_counter['watcher'] += 1

            self.watcher = watcher_class(worker_id=watcher_id,
                                         shared_memory_pointer=self.shared_memory,
                                         comm_driver=self.comm_driver,
                                         proxy_comm_drivers=self.proxy_comm_drivers,
                                         sleep_duration=self.config.sleep_duration,
                                         max_batches=self.config.max_batches,
                                         **kwargs)

            self.watcher.daemon = True
            self.watcher.start()

            return watcher_id
        else:
            return None

    def start_writer(self, writer_class, generator_id, **kwargs):
        if issubclass(writer_class, BaseWriter):
            writer_id = self.worker_counter['writer']
            self.worker_counter['writer'] += 1

            config = self.config.writer_config[writer_id]

            data_src = self.generators[generator_id].generate()

            self._extend_array(self.writers, writer_id)
            self.writers[writer_id] = writer_class(io_ctlr=config['io_ctlr'],
                                                   worker_id=writer_id,
                                                   comm_driver=self.comm_driver,
                                                   data_src=data_src,
                                                   data_dst=config['data_dst'],
                                                   pre_write_function=config.get('pre_write_function'),
                                                   meta={'generator_id': generator_id},

                                                   max_batches=self.config.max_batches,
                                                   sleep_duration=self.config.sleep_duration,
                                                   read_batches_per_epoch=self.config.read_batches_per_epoch,
                                                   **kwargs)

            self.writers[writer_id].daemon = True
            self.writers[writer_id].start()

            return writer_id
        else:
            return None

    def start_queues(self):
        # TODO - wghilliard - are this a reasonable way to start the comm_driver?
        self.comm_driver = QueueCommDriver({
            'ctl'   : self.config.q_multiplier * self.config.n_readers,
            'in'    : self.config.q_multiplier * self.config.n_readers,
            'malloc': self.config.q_multiplier * self.config.n_readers,
            'out'   : self.config.q_multiplier * self.config.n_readers
        })

        for _ in range(self.config.n_generators + len(self.config.writer_config or [])):
            self.proxy_comm_drivers.append(QueueCommDriver({'out': self.config.q_multiplier * self.config.n_readers,
                                                            'ctl': self.comm_driver.comm_handles['ctl']}))

    def stop_queues(self):
        if self.comm_driver:
            self.comm_driver.stop()
            self.comm_driver = None

        for comm_driver in self.proxy_comm_drivers:
            try:

                comm_driver.stop()
            except Exception as e:
                data_provider_logger.error(e)


        self.proxy_comm_drivers = list()

    def hard_stop(self):
        try:
            self.comm_driver.write('ctl', EXIT)
        except AttributeError:
            pass
        except Exception as e:
            raise_with_traceback(e)

        self.drain_queues()
        self.stop_queues()

        try:
            self.filler.join(timeout=5)
        except Exception as e:
            data_provider_logger.debug(e)

        for reader in self.readers:
            try:
                reader.join(timeout=5)
            except Exception as e:
                data_provider_logger.debug(e)

        if self.watcher is not None:
            try:
                self.watcher.join(timeout=5)
            except Exception as e:
                data_provider_logger.debug(e)

        # attempting to free memory, according to the internet, this may or may not actually work.
        # del self.shared_memory
        # gc.collect()
        self.is_started = False
        data_provider_logger.debug(' successfully stopped...')

    def check_errors_and_stop(self, *args, **kwargs):
        workers = [self.filler] + self.readers + [self.watcher] + self.generators + self.writers

        for worker in workers:
            try:
                with worker.error.get_lock():
                    if worker.error.value:
                        data_provider_logger.error(worker.error.value)
            except AttributeError:
                pass
            except Exception as e:
                raise_with_traceback(e)

        self.hard_stop()
        raise DataProviderError()

    def drain_queues(self):
        if self.comm_driver:
            self.comm_driver.drain(['out', 'in', 'malloc'])

        for comm_driver in self.proxy_comm_drivers:
            comm_driver.drain(['out'])

    def stop_reader(self):
        self.comm_driver.write('ctl', PRUNE)

    def first(self):
        if isinstance(self.generators[0], BaseGenerator):
            return self.generators[0]
        else:
            return None

    def writers_have_stopped(self):
        codes = range(len(self.writers))
        for index, writer in enumerate(self.writers):
            with writer.stop.get_lock():
                codes[index] = writer.stop.value

        return all(codes)

    def workers_have_stopped(self):
        codes = range(sum(self.worker_counter.values()))
        for index, worker in enumerate([self.filler] + self.readers + [self.watcher] + self.generators + self.writers):
            with worker.stop.get_lock():
                codes[index] = worker.stop.value

        return all(codes)

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

    @staticmethod
    def translate_old_sample_spec(old_sample_spec):
        return


class H5FileDataProvider(FileDataProvider):
    def start(self, **kwargs):
        return super(H5FileDataProvider, self).start(FileFiller, FileReader, BaseGenerator,
                                                     filler_io_ctlr=IOController,
                                                     reader_io_ctlr=IOController,
                                                     shape_reader_class=FileReader,
                                                     **kwargs)


class WatchedH5FileDataProvider(FileDataProvider):
    def start(self, **kwargs):
        return super(WatchedH5FileDataProvider, self).start(FileFiller, FileReader, BaseGenerator,
                                                            filler_io_ctlr=IOController,
                                                            reader_io_ctlr=IOController,
                                                            watcher_class=BaseWatcher,
                                                            shape_reader_class=FileReader, **kwargs)

