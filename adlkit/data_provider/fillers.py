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
import copy
import logging as lg
import time

from numpy import random

from .config import FILLER_OFFSET
from .io_drivers import DataIODriver, IOController
from .workers import Worker

RANDOM = random.random

filler_logger = lg.getLogger('data_provider.workers.fillers')


class BaseFiller(Worker):
    # io_driver = None

    def __init__(self,
                 comm_driver,
                 # io_driver,
                 io_ctlr,
                 worker_id,
                 read_batches_per_epoch=None,
                 max_batches=None, **kwargs):
        super(BaseFiller, self).__init__(worker_id + FILLER_OFFSET, comm_driver, **kwargs)

        self.max_batches = max_batches
        self.read_batches_per_epoch = read_batches_per_epoch
        self.data_set_tracker = dict()

        # assert isinstance(io_driver, DataIODriver)
        # self.io_driver = io_driver

        assert isinstance(io_ctlr, IOController)
        self.io_ctlr = io_ctlr

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
        with self.io_ctlr:
            while not self.should_stop():
                if self.read_batches_per_epoch is not None and self.batch_count % self.read_batches_per_epoch == 0:
                    self.reset()

                start_time = time.time()
                self.debug("start build_batch")
                batch = self.build_batch()
                self.debug("build_batch_time={0}".format(time.time() - start_time))

                if batch is None:
                    return False

                # put cannot be trusted, must poll manually

                in_queue_put_wait_time = time.time()
                self.debug("start comm_driver.in.put")
                while not self.should_stop():
                    success = self.comm_driver.write('in', batch, block=False)
                    if not success:
                        self.debug("comm_driver['in'] is full, sleeping")
                        self.sleep()
                    else:
                        self.debug("successfully wrote data to comm_driver['in']")
                        break

                self.debug(
                        "batch_fill_time={0} in_queue_put_wait_time={1}".format(time.time() - start_time,
                                                                                time.time() - in_queue_put_wait_time))
                self.batch_count += 1

        self.debug("exiting...")
        self.seppuku()


class FileFiller(BaseFiller):
    """

    """

    # TODO data_sets required
    def __init__(self, classes, class_index_map, comm_driver, worker_id,
                 read_size,
                 data_sets,
                 file_index_list,
                 read_batches_per_epoch=None,
                 max_batches=None,
                 filter_function=None,
                 skip=0,
                 wrap_examples=False,
                 shape_reader=None,
                 suppress_opens=False,
                 **kwargs):

        super(FileFiller, self).__init__(worker_id=worker_id,
                                         max_batches=max_batches,
                                         comm_driver=comm_driver,
                                         read_batches_per_epoch=read_batches_per_epoch,
                                         **kwargs)
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
        self.suppress_opens = suppress_opens
        self.filler_id = self.worker_id - FILLER_OFFSET

        self.file_index_list = file_index_list

        self.report = True

    def inform_data_provider(self, data_sets, batch):
        malloc_requests = list()
        if self.shape_reader is None:
            file_name = self.file_index_list[batch[0][0]]
            io_driver = self.io_ctlr(file_name)

            with io_driver:

                data_handle = io_driver.get(file_name)

                for data_set in data_sets:
                    shape = data_handle[data_set][0].shape
                    malloc_requests.append((data_set, shape))

                io_driver.close(file_name, data_handle)

        else:
            payloads = self.shape_reader.process_batch(batch, store_in_shared=False)
            for item_index, item in enumerate(payloads):
                shape = item.shape[1:]
                name = 'inferred_{}'.format(item_index)
                malloc_requests.append((name, shape))

        self.comm_driver.write('malloc', malloc_requests)

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
        io_driver = self.io_ctlr('lmdb+proto')
        with io_driver:
            for class_name in self.classes:

                tmp_class_holder = self.classes[class_name]

                tmp_examples_added = 0
                while tmp_examples_added < tmp_class_holder['n_examples']:
                    if tmp_class_holder['example_index'] == 0:
                        file_name = tmp_class_holder['file_names'][tmp_class_holder['file_index']]
                        file_name = self.file_index_list[file_name]

                        self.debug(['Opening:', class_name, str(tmp_class_holder['file_index']), file_name])

                        data_handle = io_driver.get(file_name, suppress=self.suppress_opens)

                        for data_set in tmp_class_holder['data_set_names']:
                            if data_set not in tmp_data_set_tracker:
                                tmp_data_set_tracker.append(data_set)

                        # TODO - wghilliard - implement the assert
                        # assert (sum(tmp_list) != len(tmp_list) * tmp_list[0],
                        #         "{0} has datasets with mismatched tensor shapes".format(file_name))

                        filter_time = time.time()
                        if self.filter_function is not None:
                            tmp_filter_index_list = self.filter_function[class_name](data_handle,
                                                                                     tmp_class_holder["data_set_names"])
                        else:
                            tmp_filter_index_list = range(data_handle[tmp_class_holder["data_set_names"][0]].shape[0])

                        self.debug("filter_function_time={0}".format(time.time() - filter_time))

                        tmp_class_holder['current_file_indices'] = sorted(tmp_filter_index_list)

                        io_driver.close(file_name, data_handle)

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
