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

import logging as lg
from abc import ABCMeta, abstractmethod

import h5py
import numpy as np


class DataIODriver(object):
    __metaclass__ = ABCMeta

    def __init__(self, opts=None):
        self.opts = opts or dict()

    def init(self):
        pass

    # TODO does it make sense to use the enter?
    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abstractmethod
    def get(self, *args, **kwargs):
        return

    @abstractmethod
    def put(self, *args, **kwargs):
        return

    @abstractmethod
    def close(self, *args, **kwargs):
        return


class FileDataIODriver(DataIODriver):
    file_handle_holder = None
    cache_handles = False

    read_batches_per_epoch = None

    def __init__(self, opts=None):
        super(FileDataIODriver, self).__init__(opts)
        self.file_handle_holder = dict()
        self.cache_handles = self.opts.get("cache_handles", False)

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        for handle in self.file_handle_holder.values():
            try:
                handle.close()
            except Exception as e:
                lg.error(e)

    def get(self, descriptor, suppress=False, *args, **kwargs):
        if self.cache_handles:
            if descriptor in self.file_handle_holder:
                return self.file_handle_holder[descriptor]
            else:
                file_handle = self.file_handle_holder[descriptor] = self._get(descriptor)
                return file_handle
        else:
            return self._get(descriptor, suppress=suppress, *args, **kwargs)

    def put(self, descriptor, read_batches_per_epoch=None, *args, **kwargs):
        self.read_batches_per_epoch = read_batches_per_epoch
        if self.cache_handles:
            if descriptor in self.file_handle_holder:
                return self.file_handle_holder[descriptor]
            else:
                file_handle = self.file_handle_holder[descriptor] = self._put(descriptor)
                return file_handle
        else:
            return self._put(descriptor, *args, **kwargs)

    def close(self, descriptor, handle, force=False):
        if not self.cache_handles or force:
            self._close(handle)
            if descriptor in self.file_handle_holder:
                self.file_handle_holder[descriptor] = None

    def _get(self, descriptor, suppress=False, *args, **kwargs):
        return

    def _put(self, descriptor, *args, **kwargs):
        return

    def _close(self, handle, *args, **kwargs):
        return


class H5FileWrapper(object):
    _data = None
    read_batches_per_epoch = None

    def __init__(self, descriptor, mode, read_batches_per_epoch=None):
        self._data = h5py.File(descriptor, mode)
        self.read_batches_per_epoch = read_batches_per_epoch

    def __getitem__(self, item):
        payload = self._data[item]

        # TODO - wghilliard - add support for recursive groups
        if isinstance(payload, h5py.Group):
            return [payload[key] for key in sorted(payload.keys(), key=lambda index: int(index))]
        elif isinstance(payload, (h5py.Dataset, np.ndarray)):
            return payload
        else:
            msg = 'H5FileWrapper not sure how to handle type(payload)={}'.format(type(payload))
            raise TypeError(msg)

        # if isinstance(item, (list, tuple)) and len(item) > 0:
        #     if isinstance(item[0], int) and isinstance(item[-1], int):
        #         return self._data[item]
        #     elif isinstance(item[0], str) and isinstance(item[-1], str):
        #         pass

    def __setitem__(self, key, value):
        assert self._data is not None
        # if isinstance(key, (list, tuple)) and len(key) > 0:
        #     if isinstance(key[0], str) and isinstance(key[-1], str):
        #         return

        if isinstance(value, (list, tuple)):
            group = self._data.require_group(key)
            # TODO - wghilliard - add support for recursive groups
            for index, item in enumerate(value):
                sub_key = str(index)
                assert isinstance(item, np.ndarray)
                # key = '{}/{}'.format(key, index)
                if sub_key not in group.keys():
                    axis_0 = self.read_batches_per_epoch or 1
                    data_set = group.create_dataset(sub_key,
                                                    shape=((axis_0,) + item.shape),
                                                    maxshape=((None,) + item.shape),
                                                    dtype=item.dtype,
                                                    chunks=True)
                else:
                    data_set = group[sub_key]
                    group[sub_key].resize((data_set.shape[0] + 1,) + data_set.shape[1:])

                data_set[-1] = item

        elif isinstance(value, np.ndarray):
            self._data.require_dataset(key, data=value, chunks=True)

        else:
            msg = 'H5FileWrapper not sure how to handle type(payload)={}'.format(type(payload))
            raise TypeError(msg)

        return True

    def close(self):
        try:
            return self._data.close()
        except AttributeError:
            return None


class H5DataIODriver(FileDataIODriver):
    def _get(self, descriptor, suppress=False, **kwargs):
        # return h5py.File(descriptor, 'r')
        return H5FileWrapper(descriptor=descriptor,
                             mode='r',
                             read_batches_per_epoch=self.read_batches_per_epoch)

    def _put(self, descriptor, *args, **kwargs):
        # return h5py.File(descriptor, 'a')
        return H5FileWrapper(descriptor=descriptor,
                             mode='a',
                             read_batches_per_epoch=self.read_batches_per_epoch)

    def _close(self, handle, *args, **kwargs):
        return handle.close()
