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
import ctypes
import datetime
import logging as lg
import multiprocessing
from unittest import TestCase

import numpy as np

from adlkit.data_provider.comm_drivers import QueueCommDriver
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

        comm_driver = QueueCommDriver({
            'ctl': 10,
            'in' : 100,
            'out': 100
        })

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

        reader = FileReader(worker_id=reader_id,
                            # controller_socket_str=controller_socket_str,
                            # sync_str=sync_str,
                            # in_queue_str=in_queue_str,
                            # out_queue_str=out_queue_str,
                            comm_driver=comm_driver,
                            shared_memory_pointer=shared_data_pointer,
                            max_batches=max_batches, read_size=read_size,
                            class_index_map=mock_class_index_map,
                            file_index_list=mock_file_index_list,
                            io_driver=H5DataIODriver())

        # building queue up with read requests
        for batch in mock_batches:
            success = comm_driver.write('in', batch, block=False)
            self.assertTrue(success)

        reader.read()

        out = list()
        start_time = datetime.datetime.utcnow()
        end_time = datetime.timedelta(seconds=10) + start_time
        while datetime.datetime.utcnow() < end_time and len(out) != max_batches:
            batch = comm_driver.read('out', block=False)

            if batch is not None:
                out.append(batch)

        self.assertEquals(len(out), max_batches,
                          "test consumed {0} of {1} expected batches from the in_queue".format(
                                  len(out),
                                  max_batches))

    def test_file_caching(self):

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
        # in_queue = multiprocessing.Queue(maxsize=max_batches)
        # out_queue = multiprocessing.Queue(maxsize=max_batches)

        comm_driver = QueueCommDriver({
            'ctl': 10,
            'in' : 100,
            'out': 100
        })

        # building queue up with read requests
        for batch in mock_batches:
            success = comm_driver.write('in', batch)
            self.assertTrue(success)

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

        reader = FileReader(worker_id=reader_id,
                            comm_driver=comm_driver,
                            shared_memory_pointer=shared_data_pointer,
                            max_batches=max_batches, read_size=read_size,
                            class_index_map=mock_class_index_map,
                            file_index_list=mock_file_index_list,
                            io_driver=H5DataIODriver({"cache_handles": True}))

        reader.read()

        out = list()
        start_time = datetime.datetime.utcnow()
        end_time = datetime.timedelta(seconds=10) + start_time
        while datetime.datetime.utcnow() < end_time and len(out) != max_batches:
            batch = comm_driver.read('out', block=False)
            if batch is not None:
                out.append(batch)

        self.assertEquals(len(out), max_batches,
                          "test consumed {0} of {1} expected batches from the in_queue".format(
                                  len(out),
                                  max_batches))

        target = 0
        self.assertGreater(len(reader.io_driver.file_handle_holder), 0)
        for handle in reader.io_driver.file_handle_holder:
            # Here we try to close the file again, this will raise an Exception and thus means the caching cleaned up
            # successfully. if 0 then we closed everything successfully with the __exit__ function
            target += 1
            try:
                handle.close()
            except Exception:
                target -= 1

        self.assertEqual(target, 0)

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
        # in_queue = multiprocessing.Queue(maxsize=max_batches)
        # out_queue = multiprocessing.Queue(maxsize=max_batches)

        comm_driver = QueueCommDriver({
            'ctl': 10,
            'in' : 100,
            'out': 100
        })

        # building queue up with read requests
        for batch in mock_batches:
            success = comm_driver.write('in', batch)
            self.assertTrue(success)

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

        reader = FileReader(worker_id=reader_id,
                            comm_driver=comm_driver,
                            shared_memory_pointer=shared_data_pointer,
                            max_batches=max_batches, read_size=read_size,
                            class_index_map=mock_class_index_map,
                            make_one_hot=True, make_class_index=True,
                            file_index_list=mock_file_index_list,
                            io_driver=H5DataIODriver())

        reader.read()

        out = list()
        start_time = datetime.datetime.utcnow()
        end_time = datetime.timedelta(seconds=10) + start_time
        while datetime.datetime.utcnow() < end_time and len(out) != max_batches:
            batch = comm_driver.read('out', block=False)
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
        comm_driver = QueueCommDriver({
            'ctl': 10,
            'in' : 100,
            'out': 100
        })

        # building queue up with read requests
        for batch in mock_filtered_batches:
            success = comm_driver.write('in', batch)
            self.assertTrue(success)

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

        reader = FileReader(worker_id=reader_id,
                            comm_driver=comm_driver,
                            shared_memory_pointer=shared_data_pointer,
                            max_batches=max_batches, read_size=read_size,
                            class_index_map=mock_class_index_map,
                            file_index_list=mock_file_index_list,
                            make_one_hot=True,
                            make_class_index=True,
                            io_driver=H5DataIODriver())

        reader.read()

        out = list()
        start_time = datetime.datetime.utcnow()
        end_time = datetime.timedelta(seconds=10) + start_time
        while datetime.datetime.utcnow() < end_time and len(out) != max_batches:
            batch = comm_driver.read('out', block=False)
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
        comm_driver = QueueCommDriver({
            'ctl': 10,
            'in' : 100,
            'out': 100
        })

        # building queue up with read requests
        for batch in mock_batches:
            success = comm_driver.write('in', batch)
            self.assertTrue(success)

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

        reader = FileReader(worker_id=reader_id,
                            comm_driver=comm_driver,
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
        start_time = datetime.datetime.utcnow()
        end_time = datetime.timedelta(seconds=10) + start_time
        while datetime.datetime.utcnow() < end_time and len(out) != max_batches:
            batch = comm_driver.read('out', block=False)
            if batch is not None:
                out.append(batch)

        self.assertEquals(len(out), max_batches,
                          "test consumed {0} of {1} expected batches from the in_queue".format(
                                  len(out),
                                  max_batches))
