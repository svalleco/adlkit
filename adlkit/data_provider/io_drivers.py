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
from abc import ABCMeta, abstractmethod

import h5py
import numpy as np
from future.utils import raise_with_traceback

from adlkit.data_catalog.abstract_data_catalog import DataPointer


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
    protocol = 'file'
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
        if not self.cache_handles:
            self.close_all()

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
            if descriptor and descriptor in self.file_handle_holder:
                del self.file_handle_holder[descriptor]

    def close_all(self):
        for descriptor, handle in self.file_handle_holder.items():
            self.close(descriptor, handle)
        self.file_handle_holder = dict()

    def _get(self, descriptor, suppress=False, *args, **kwargs):
        return

    def _put(self, descriptor, *args, **kwargs):
        return

    def _close(self, handle, *args, **kwargs):
        return


class H5FileWrapper(object):
    _data = None
    axis_0 = None

    def __init__(self, descriptor, mode, axis_0=None):
        self._data = h5py.File(descriptor, mode)
        self.axis_0 = axis_0 or 1

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

                    data_set = group.create_dataset(sub_key,
                                                    shape=((self.axis_0,) + item.shape),
                                                    maxshape=((None,) + item.shape),
                                                    dtype=item.dtype,
                                                    chunks=True)
                else:
                    data_set = group[sub_key]
                    group[sub_key].resize((data_set.shape[0] + 1,) + data_set.shape[1:])

                data_set[-1] = item

        elif isinstance(value, np.ndarray):

            if key not in self._data.keys():
                data_set = self._data.create_dataset(key,
                                                     shape=((self.axis_0,) + value.shape),
                                                     maxshape=((None,) + value.shape),
                                                     dtype=value.dtype,
                                                     chunks=True)
            else:
                data_set = self._data[key]
                data_set.resize((data_set.shape[0] + 1,) + data_set.shape[1:])

            # TODO - wghilliard - write at index
            data_set[-1] = value

        else:
            msg = 'H5FileWrapper not sure how to handle type(value)={}'.format(type(value))
            raise TypeError(msg)

        return True

    def close(self):
        try:
            return self._data.close()
        except AttributeError:
            return None

    def keys(self):
        return self._data.keys()

    @property
    def filename(self):
        return self._data.filename
        # TODO - wghilliard error handling
        # if self._data:
        #     return se


class H5DataIODriver(FileDataIODriver):
    protocol = 'h5'

    def _get(self, descriptor, suppress=False, **kwargs):
        # return h5py.File(descriptor, 'r')
        return H5FileWrapper(descriptor=descriptor,
                             mode='r',
                             axis_0=self.read_batches_per_epoch)

    def _put(self, descriptor, *args, **kwargs):
        # return h5py.File(descriptor, 'a')
        return H5FileWrapper(descriptor=descriptor,
                             mode='a',
                             axis_0=self.read_batches_per_epoch)

    def _close(self, handle, *args, **kwargs):
        return handle.close()


class Controller(object):
    drivers = dict()
    opts = dict()

    is_initialized = False

    # def __new__(cls, *args, **kwargs):

    def __init__(self, **kwargs):
        self.opts = self.opts or {
            'keep_alive'   : True,
            'cache_handles': True,
            'auto_init'    : True,
            'default'      : 'h5'
        }
        self.opts.update(kwargs)
        if self.opts.get('auto_init') and not self.is_initialized:
            self.init()

        self.drivers['default'] = self.drivers[self.opts.get('default')]
        self.is_initialized = False

    def init(self):
        for key, driver_cls in self.drivers.items():
            try:
                self.drivers[key] = driver_cls(self.opts)
            except TypeError:
                pass
            except Exception as e:
                raise_with_traceback(e)

        self.is_initialized = True


class IOController(Controller):

    def __call__(self, url, *args, **kwargs):
        return self.parse_out_driver(url)

    # @contextmanager
    def parse_out_driver(self, url):
        if url is not None:
            # source_type, source_name, data_set_list, index = DataPointer._parse_url(url)
            source_type = url.split('://')[0]
            try:
                return self.drivers[source_type]
            except KeyError:
                return self.drivers['default']

        elif not self.opts.get('auto_init'):
            self.init()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for name, driver in self.drivers.items():
            # try:
            driver.close_all()
            # except as e:
            #     print(e)

    def data_pointer_url_to_driver_handle(self, url):
        source_type, source_name, data_set_list, index = DataPointer._parse_url(url)

        driver = self.drivers[source_type]
        return driver.get(source_name)


Controller.drivers[H5DataIODriver.protocol] = H5DataIODriver
