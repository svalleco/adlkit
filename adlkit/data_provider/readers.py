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
import logging as lg
import time

import keras
import numpy as np
from future.utils import raise_with_traceback

from adlkit.data_provider.comm_drivers import BaseCommDriver
from .config import READER_OFFSET
from .io_drivers import DataIODriver, IOController
from .workers import Worker

# lg.basicConfig(level=lg.INFO)

reader_logger = lg.getLogger('data_provider.workers.readers')

EXIT = object()


class BaseReader(Worker):
    io_driver = None

    def __init__(self, worker_id, comm_driver, shared_memory_pointer, read_size,
                 # io_driver,
                 io_ctlr,
                 max_batches=None, **kwargs):
        """
        :param worker_index:
        :param in_queue_str:
        :param out_queue_str:
        :param shared_memory_pointer:
        """

        # TODO not sure if this is the correct syntax
        # https://github.com/numpy/numpy/blob/master/numpy/core/memmap.py
        assert isinstance(comm_driver, BaseCommDriver)
        # assert isinstance(io_driver, DataIODriver)
        assert isinstance(io_ctlr, IOController)
        super(BaseReader, self).__init__(worker_id=worker_id + READER_OFFSET,
                                         comm_driver=comm_driver,
                                         **kwargs)

        self.shared_memory_pointer = shared_memory_pointer
        self.max_batches = max_batches
        self.read_size = read_size
        self.reader_id = self.worker_id - READER_OFFSET

        # self.io_driver = io_driver
        self.io_ctlr = io_ctlr

    def debug(self, message):
        if isinstance(message, list):
            message = " ".join(message)

        reader_logger.debug(" reader_id={0} ".format(self.worker_id) + message)

    def info(self, message):
        if isinstance(message, list):
            message = " ".join(message)
        reader_logger.info(" reader_id={0} reader_batch_id={1} ".format(self.worker_id,
                                                                        self.batch_count) + message)

    def critical(self, message):
        if isinstance(message, list):
            message = " ".join(message)
        reader_logger.critical(" reader_id={0} reader_batch_id={1} ".format(self.worker_id,
                                                                            self.batch_count) + message)

    def error(self, message):
        if isinstance(message, list):
            message = " ".join(message)
        reader_logger.error(" reader_id={0} reader_batch_id={1} ".format(self.worker_id,
                                                                         self.batch_count) + message)

    def run(self, **kwargs):
        self.read()

    def read(self):
        self.debug("starting...")
        with self.io_ctlr:

            in_queue_time = time.time()

            while not self.should_stop():
                # TODO - wghilliard - does strictly reading from the comm_driver make sense?
                # batch = self.get_batch()
                batch = self.comm_driver.read('in', block=False)
                if batch is not None:
                    self.debug("in_queue_get_wait_time={0}".format(time.time() - in_queue_time))
                    start_time = time.time()
                    self.debug("starting to prepare batch")
                    data_pointer = self.process_batch(batch)
                    self.debug("process_batch_time={0}".format(time.time() - start_time))

                    if data_pointer is None:
                        self.critical("process_batch returned None, exiting... {0}".format(batch))
                        return False

                    elif data_pointer is EXIT:
                        break

                    self.debug("starting out_queue.put")
                    wait_time = time.time()
                    while not self.should_stop():
                        success = self.comm_driver.write('out', data_pointer, block=False)
                        if not success:
                            self.debug("comm_driver['out'] is full, sleeping")
                            self.sleep()
                        else:
                            self.debug("successfully wrote data to comm_driver['out']")
                            break

                    self.debug("batch_read_time={0} out_queue_put_wait_time={1}".format(time.time() - start_time,
                                                                                        time.time() - wait_time))

                    self.batch_count += 1
                    in_queue_time = time.time()
                else:
                    self.sleep()

        self.debug("exiting...")
        self.seppuku()

    def process_batch(self, batch, **kwargs):
        return batch

    def find_bucket(self):
        self.debug("attempting to find a bucket")
        start_time = time.time()
        try:
            while not self.should_stop():
                for bucket_index, bucket in enumerate(self.shared_memory_pointer):
                    with bucket[0].get_lock():
                        if bucket[0].value == 0:
                            bucket[0].value = 1
                            self.debug("bucket_seek_time={0} bucket_index={1}".format(time.time() - start_time,
                                                                                      bucket_index))
                            assert bucket_index is not None
                            return bucket_index
                self.sleep()
        except Exception as e:
            self.error("cannot get a bucket")
            self.error(e)
            raise_with_traceback(e)
        return None


class FileReader(BaseReader):
    def __init__(self, worker_id, comm_driver, shared_memory_pointer, read_size, class_index_map, file_index_list,
                 io_ctlr,
                 max_batches=None,
                 process_function=None,
                 make_class_index=False,
                 make_one_hot=False,
                 make_file_index=False,
                 shuffle=True,

                 **kwargs):
        super(self.__class__, self).__init__(worker_id=worker_id,
                                             comm_driver=comm_driver,
                                             shared_memory_pointer=shared_memory_pointer,
                                             read_size=read_size,
                                             io_ctlr=io_ctlr,
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

        assert isinstance(io_ctlr, IOController)
        self.io_ctl = io_ctlr

    def process_batch(self, batch, store_in_shared=True):
        batch_id = 0
        if store_in_shared:
            bucket_index = self.find_bucket()
            try:
                assert isinstance(bucket_index, int)
            except AssertionError:
                self.debug("reader shared memory bucket=None, if DataProvider.hard_stop was called then ignore")
                return EXIT
        else:
            bucket_index = 0
        data_sets = list()

        # saving memory address of shared_memory_pointer for check later
        # tmp = hex(id(self.shared_memory_pointer[0][0][1][0]))

        payloads = collections.OrderedDict()

        tmp_file_struct = list()

        tmp_index_payload = None
        start = 0
        n_read_requests = len(batch)

        payloads_build_time = time.time()
        for read_index, read_request in enumerate(batch):
            file_path_index, data_sets, class_name, read_descriptor, batch_id = read_request

            file_path = self.file_index_list[file_path_index]
            io_driver = self.io_ctlr(file_path.split('.')[-1])
            with io_driver:
                data_handle = io_driver.get(file_path)

                io_driver_to_payloads_time = time.time()
                for data_set in data_sets:
                    try:
                        payloads[data_set][read_index]
                    except KeyError:
                        payloads[data_set] = range(n_read_requests)

                    if isinstance(read_descriptor, tuple):
                        # TODO use read_direct function instead
                        # http://docs.h5py.org/en/latest/high/dataset.html
                        payloads[data_set][read_index] = np.array(
                                data_handle[data_set][read_descriptor[0]:read_descriptor[1]])

                        # payloads[data_set_index][read_index] = h5_file_handle[data_set][read_descriptor[
                        # 0]:read_descriptor[1]]

                    elif isinstance(read_descriptor, list) or isinstance(read_descriptor, np.ndarray):
                        # TODO there is potentially a faster way
                        # https://stackoverflow.com/questions/21766145/h5py-correct-way-to-slice-array-datasets
                        # payloads[data_set][read_index] = np.take(h5_file_handle[data_set], read_descriptor, axis=0)
                        payloads[data_set][read_index] = data_handle[data_set][read_descriptor]

                self.debug("io_driver_to_payloads_time={0} read_index={1} batch_id={2}".format(time.time() -
                                                                                               io_driver_to_payloads_time,
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
                    raise_with_traceback(e)

                start += n_examples

                io_driver.close(file_path, data_handle)

        self.debug("payloads_build_time={0} batch_id={1}".format(time.time() - payloads_build_time, batch_id))

        concat_time = time.time()
        for data_set in payloads:
            try:
                payloads[data_set] = np.concatenate(payloads[data_set])
            except ValueError as e:
                try:
                    payloads[data_set] = sum(payloads[data_set], list())
                except Exception as ee:

                    lg.error(e)
                    lg.error(ee)
                    lg.error(payloads[data_set])
                    raise ValueError(e)

        self.debug("concat_time={0} batch_id={1}".format(time.time() - concat_time,
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
            self.debug("process_function_time={0} batch_id={1}".format(time.time() - process_time,
                                                                       batch_id))
        else:
            payloads = payloads.values()

        if self.shuffle:
            payloads = self.shuffle_in_unison_inplace(payloads)

        if store_in_shared:
            store_in_shared_time = time.time()
            for data_set_index, payload in enumerate(payloads):
                try:
                    self.shared_memory_pointer[bucket_index][1][data_set_index][...] = np.copy(payload)
                except TypeError as e:
                    self.critical(e.message)
                    self.critical(
                            "SHARED MEMORY FAILED, bucket_index={0} data_set_index={1} batch={2} payload={3}".format(
                                    bucket_index,
                                    data_set_index,
                                    batch,
                                    payload))
                except IndexError as e:
                    raise_with_traceback(e)

            self.debug("store_in_shared_time={0} batch_id={1}".format(time.time() - store_in_shared_time,
                                                                      batch_id))

            return bucket_index, data_sets, batch_id
        else:
            return payloads

    def shuffle_in_unison_inplace(self, payloads):
        shuffle_list = np.random.permutation(self.read_size)

        out = []
        for payload in payloads:
            assert self.read_size == len(payload)
            out.append(payload[shuffle_list])

        return out
