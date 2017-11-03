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
from datetime import datetime

from adlkit.data_catalog.utils import epoch_time_to_file_name, timestamp_to_epoch


class Label(object):
    timestamp = None
    str_start_time = None
    epoch_start_time = None

    version = None
    ips = None

    label_name = None
    class_name = None
    label_family = None

    file_path = None
    data_sets = []
    indices = None

    def __init__(self, init_dict):
        # TODO thing about how else might we instantiate this, maybe load json?
        # idk

        # TODO
        # also, what attributes should be computed if not given?
        if isinstance(init_dict, dict):
            self.from_dict(init_dict)

        # a required attribute, all other info is derived from it.
        self.timestamp = self.timestamp or datetime.utcnow()

        if not (self.epoch_start_time or self.str_start_time):
            self.epoch_start_time = timestamp_to_epoch(self.timestamp)

        if not self.str_start_time:
            self.str_start_time = epoch_time_to_file_name(self.epoch_start_time)

            # self.epoch_start_time = self.epoch_start_time or \
            #                         file_name_to_epoch_time(self.str_start_time)

    def from_dict(self, init_dict):
        for key, value in init_dict.items():
            setattr(self, key, value)

    def to_dict(self):
        return self.__dict__

    def to_sample_spec(self, format_struct, class_name=None):
        out = list()

        self.class_name = class_name or self.label_name
        for item in format_struct:
            try:
                tmp = format_struct[item](getattr(self, item))
            except AttributeError:
                lg.error("the label does not contain the info required by the format")
                tmp = format_struct[item]()
            except ValueError:
                raise ValueError
            except TypeError:
                lg.error("the label does not contain the info required by the format")
                tmp = format_struct[item]()

            out.append(tmp)

        return out


class DataAPI(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    # TODO review this method
    @abstractmethod
    def insert(self, str_start_time, label, file_path, indices):
        pass

    @abstractmethod
    def insert_by_label(self, label):
        pass

    @abstractmethod
    def get_by_label(self, label):
        pass

    @abstractmethod
    def get_by_time(self, start, end):
        pass

    @abstractmethod
    def get_by_dict(self, search_dict):
        pass

    @abstractmethod
    def generate_batch(self):
        pass
