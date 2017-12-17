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
import datetime
import logging as lg
from unittest import TestCase

from adlkit.data_provider.comm_drivers import QueueCommDriver
from adlkit.data_provider.fillers import FileFiller
from adlkit.data_provider.io_drivers import H5DataIODriver

lg.basicConfig(level=lg.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s ')
test_logger = lg.getLogger('data_providers.tests')
test_logger.setLevel(lg.DEBUG)


class TestFileFiller(TestCase):
    def test_fill(self):

        from mock_config import mock_classes, mock_class_index_map, \
            mock_data_sets, mock_file_index_list

        mock_classes = copy.deepcopy(mock_classes)
        mock_class_index_map = copy.deepcopy(mock_class_index_map)
        mock_data_sets = copy.deepcopy(mock_data_sets)

        max_batches = 10
        batch_size = 500
        read_size = batch_size * 2

        comm_driver = QueueCommDriver({
            'ctl'   : 10,
            'in'    : 100,
            'malloc': 100
        })

        filler = FileFiller(classes=mock_classes,
                            class_index_map=mock_class_index_map,
                            comm_driver=comm_driver,
                            worker_id=1,
                            read_size=read_size,
                            max_batches=max_batches,
                            data_sets=mock_data_sets,
                            file_index_list=mock_file_index_list,
                            wrap_examples=True,
                            io_driver=H5DataIODriver()
                            )

        filler.fill()

        out = list()
        start_time = datetime.datetime.utcnow()
        end_time = datetime.timedelta(seconds=10) + start_time
        while datetime.datetime.utcnow() < end_time:
            batch = comm_driver.read('in', block=False)
            if batch is not None:
                out.append(batch)
                count = 0
                for item in batch:
                    count += item[3][1] - item[3][0]

                self.assertEquals(count, read_size,
                                  "read_batch was returned with read_size {0}, instead of batch_size {1}".format(
                                          count,
                                          batch_size))
            if len(out) == max_batches:
                break

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

        comm_driver = QueueCommDriver({
            'ctl'   : 10,
            'in'    : 100,
            'malloc': 100
        })

        filler = FileFiller(classes=mock_classes,
                            class_index_map=mock_class_index_map,
                            comm_driver=comm_driver,
                            worker_id=1,
                            read_size=read_size,
                            max_batches=max_batches,
                            data_sets=mock_data_sets,
                            file_index_list=mock_file_index_list,
                            io_driver=H5DataIODriver())

        filler.fill()

        out = list()
        start_time = datetime.datetime.utcnow()
        end_time = datetime.timedelta(seconds=10) + start_time
        while datetime.datetime.utcnow() < end_time:
            malloc_request = comm_driver.read('malloc', block=False)

            if malloc_request is not None:
                out.extend(malloc_request)

            if len(out) == len(mock_expected_malloc_requests):
                break

        self.assertEqual(len(out), len(mock_expected_malloc_requests))
        for request, expected_request in zip(out, mock_expected_malloc_requests):
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

        comm_driver = QueueCommDriver({
            'ctl'   : 10,
            'in'    : 100,
            'malloc': 100
        })

        filler = FileFiller(classes=mock_classes,
                            class_index_map=mock_class_index_map,
                            comm_driver=comm_driver,
                            worker_id=1,
                            read_size=read_size,
                            max_batches=max_batches,
                            wrap_examples=True, data_sets=mock_data_sets,
                            file_index_list=mock_file_index_list,
                            io_driver=H5DataIODriver())

        filler.fill()

        out = int()
        start_time = datetime.datetime.utcnow()
        end_time = datetime.timedelta(seconds=10) + start_time
        while datetime.datetime.utcnow() < end_time:

            batch = comm_driver.read('in', block=False)

            if batch is not None:
                out += 1
                count = 0
                for item in batch:
                    count += item[3][1] - item[3][0]

                self.assertEquals(count, read_size,
                                  "read_batch was returned with read_size {0}, instead of batch_size {1} \n {"
                                  "2}".format(
                                          count,
                                          read_size,
                                          batch))
            if out == max_batches:
                break

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
        from adlkit.data_provider.tests.mock_config import mock_classes, mock_class_index_map, \
            mock_data_sets, mock_file_index_list
        mock_classes = copy.deepcopy(mock_classes)
        mock_class_index_map = copy.deepcopy(mock_class_index_map)
        mock_data_sets = copy.deepcopy(mock_data_sets)
        mock_file_index_list = copy.deepcopy(mock_file_index_list)

        expected_batches = 2
        max_batches = 4
        batch_size = 500
        read_size = batch_size * 2
        # in_queue_str = 'ipc:///tmp/adlkit_socks_0'
        # malloc_queue_str = 'ipc:///tmp/adlkit_socks_1'
        # controller_socket_str = 'ipc:///tmp/adlkit_socks_2'

        # zmq_context = zmq.Context()
        # in_queue_socket = zmq_context.socket(zmq.PULL)
        # in_queue_socket.setsockopt(zmq.RCVHWM, 100)
        # in_queue_socket.connect(in_queue_str)
        #
        # malloc_queue_socket = zmq_context.socket(zmq.PULL)
        # malloc_queue_socket.setsockopt(zmq.RCVHWM, 100)
        # malloc_queue_socket.connect(malloc_queue_str)

        comm_driver = QueueCommDriver({
            'ctl'   : 10,
            'in'    : 100,
            'malloc': 100
        })

        filler = FileFiller(classes=mock_classes,
                            class_index_map=mock_class_index_map,
                            comm_driver=comm_driver,
                            worker_id=1,
                            read_size=read_size,
                            max_batches=max_batches,
                            wrap_examples=False,
                            data_sets=mock_data_sets,
                            file_index_list=mock_file_index_list,
                            io_driver=H5DataIODriver())

        filler.fill()

        out = int()
        start_time = datetime.datetime.utcnow()
        end_time = datetime.timedelta(seconds=10) + start_time
        while datetime.datetime.utcnow() < end_time:

            batch = comm_driver.read('in', block=False)
            if batch is not None:
                out += 1
                count = 0
                for item in batch:
                    count += item[3][1] - item[3][0]

                self.assertEquals(count, read_size,
                                  "read_batch was returned with read_size {0}, instead of batch_size {1} \n {"
                                  "2}".format(
                                          count,
                                          read_size,
                                          batch))
            if out == expected_batches:
                break

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

        comm_driver = QueueCommDriver({
            'ctl'   : 10,
            'in'    : 100,
            'malloc': 100
        })

        filler = FileFiller(classes=mock_classes,
                            class_index_map=mock_class_index_map,
                            comm_driver=comm_driver,
                            worker_id=1,
                            read_size=read_size,
                            max_batches=max_batches,
                            wrap_examples=False,
                            data_sets=mock_data_sets,
                            filter_function=mock_filter_function,
                            file_index_list=mock_file_index_list,
                            io_driver=H5DataIODriver())

        filler.fill()

        out = 0
        meow = list()
        start_time = datetime.datetime.utcnow()
        end_time = datetime.timedelta(seconds=10) + start_time
        while datetime.datetime.utcnow() < end_time:

            batch = comm_driver.read('in', block=False)

            if batch is not None:
                meow.append(batch)
                out += 1
                count = 0
                for item in batch:
                    count += len(item[3])

                self.assertEquals(count, read_size,
                                  "read_batch was returned with read_size {0}, instead of batch_size {1} \n {"
                                  "2}".format(
                                          count,
                                          read_size,
                                          batch))
            if out == max_batches:
                break
        self.assertEquals(out, max_batches,
                          "test consumed {0} of {1} expected batches from the in_queue".format(
                                  out, max_batches))
