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

dc_lg = lg.getLogger('data_catalog')


class CatalogObject(object):
    uid = None
    timestamp = None

    # TODO - wghilliard - validators
    def __init__(self, init_dict=None, **kwargs):
        self.from_dict(init_dict or {})
        self.from_dict(kwargs)

    def from_dict(self, init):
        for key, value in init.items():
            setattr(self, key, value)


# TODO - wghilliard - rename to DataPointer
class DataPointer(CatalogObject):
    source_name = None
    source_type = None
    index = None

    data_set_list = None

    def to_url(self):
        return "{}:/{}:{}:".format(self.source_type, self.source_name, ",".join(self.data_set_list), self.index)

    def from_url(self, url):
        self.source_type, self.source_name, self.data_set_list, self.index = self._parse_url(url)
        return self

    @staticmethod
    def _parse_url(url):
        source_type, rest = url.split(':/')
        source_name, data_set_list, index = rest.split(":")

        return source_type, source_name, data_set_list, index


class LabelInstance(CatalogObject):
    data_point_uid = None
    label_uid = None


class Label(CatalogObject):
    name = None
    comment = None
    is_origin = None
    # TODO - wghilliard - LabelMeta
    # username = None


class AbstractDataCatalog(object):
    __metaclass__ = ABCMeta

    opts = None

    def __init__(self):
        self.opts = dict()

    # @abstractmethod
    # def purge(self):
    #     pass
    #
    # @abstractmethod
    # def create_label(self, label):
    #     assert isinstance(label, (dict, Label))
    #
    # @abstractmethod
    # def create_label_instance(self, label_instance):
    #     assert isinstance(label_instance, (dict, LabelInstance))
    #
    # @abstractmethod
    # def create_data_point(self, data_point):
    #     assert isinstance(data_point, (dict, DataPoint))
    #
    # @abstractmethod
    # def create_data_points(self, data_points):
    #     assert isinstance(data_points, list)
    #
    # @abstractmethod
    # def delete_label(self, label):
    #     assert isinstance(label, (dict, Label, str, unicode))

    # @abstractmethod
    # def delete_data_point(self, data_point):
    #     assert isinstance(data_point, (dict, DataPoint, str, unicode))

    @abstractmethod
    def read_all_labels(self):
        pass

    #
    # @abstractmethod
    # def read_label(self, label_name):
    #     assert isinstance(label_name, (str, unicode))
    #
    # @abstractmethod
    # def read_by_label(self, label):
    #     assert isinstance(label, (str, unicode, Label))
    #
    # @abstractmethod
    # def read_by_uid(self, data_point_uid):
    #     assert isinstance(data_point_uid, (str, unicode))
    #
    # @abstractmethod
    # def read_by_uids(self, data_point_uids):
    #     assert isinstance(data_point_uids, list)
    #
    # @abstractmethod
    # def read_by_time(self, start_time, end_time):
    #     pass
    #
    # @abstractmethod
    # def read_before(self, end_time):
    #     pass
    #
    # @abstractmethod
    # def read_after(self, start_time):
    #     pass

    # @abstractmethod
    def generate_batch(self):
        raise NotImplemented
