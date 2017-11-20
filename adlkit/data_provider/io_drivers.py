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
    def get(self, *args):
        pass

    @abstractmethod
    def close(self, *args):
        pass


class FileDataIODriver(DataIODriver):
    file_handle_holder = None
    cache_handles = False

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

    def get(self, descriptor, suppress=False):
        if self.cache_handles:
            if descriptor in self.file_handle_holder:
                return self.file_handle_holder[descriptor]
            else:
                file_handle = self.file_handle_holder[descriptor] = self._get(descriptor)
                return file_handle
        else:
            return self._get(descriptor, suppress=suppress)

    def _get(self, descriptor, suppress=False):
        pass

    def close(self, descriptor, handle, force=False):
        if not self.cache_handles or force:
            self._close(handle)
            if descriptor in self.file_handle_holder:
                self.file_handle_holder[descriptor] = None

    def _close(self, handle):
        pass


class H5DataIODriver(FileDataIODriver):
    def _get(self, descriptor, suppress=False):
        return h5py.File(descriptor, 'r')

    def _close(self, handle):
        handle.close()
