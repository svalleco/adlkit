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

from __future__ import absolute_import

import glob
import os
from unittest import TestCase

from adlkit.data_catalog.old_base_data_catalog import FileDataAPI
from adlkit.data_catalog.utils import epoch_time_to_file_name, file_name_to_epoch_time


class TestFileDataAPI(TestCase):
    def setUp(self):
        self.tmp_api = FileDataAPI('./tmp', 'labels')
        try:
            os.mkdir('./tmp')
        except OSError:
            pass
        try:
            os.mkdir('./tmp/labels')
        except OSError:
            pass
        try:
            os.mkdir('./tmp/labels/base_line')
        except OSError:
            pass

            # self.tmp_file_dict = dict()
            # search_glob = "../data/*.h5"
            # tmp_file_dict = dict()
            # file_names = glob.glob(search_glob)
            # start_time_one = "2016_09_13_16_53_24"
            # start_time_two = "2016_09_13_16_54_44"
            # for file_name in file_names:
            #     try:
            #         tmp_file_dict[start_time_one].append(file_name)
            #     except KeyError:
            #         tmp_file_dict[start_time_one] = [file_name]
            #
            #     try:
            #         tmp_file_dict[start_time_two].append(file_name)
            #     except KeyError:
            #         tmp_file_dict[start_time_two] = [file_name]

    def test_init(self):
        # TODO rethink this test case
        self.assertEqual(self.tmp_api.base_dir, os.path.abspath('./tmp'))
        self.assertEqual(self.tmp_api.label_dir,
                         os.path.abspath('./tmp/labels'))

    def test_epoch_to_str_date_time(self):
        result = epoch_time_to_file_name(1473803604)
        self.assertEqual(result, '2016_09_13_16_53_24')

    def test_str_date_time_to_epoch(self):
        result = file_name_to_epoch_time('2016_09_13_16_53_24')
        self.assertEqual(result, 1473803604)

    def test_get_by_label_fail(self):
        potential_files = self.tmp_api.get_by_label('cats')
        self.assertIsNone(potential_files)

    def test_get_by_label_pass(self):
        potential_files = self.tmp_api.get_by_label('base_line')
        self.assertGreaterEqual(len(potential_files), 1)

    def test_get_by_time_consolidated(self):
        self.test_insert()
        self.test_bulk_insert()
        self.test_bulk_insert("2016_09_13_16_53_34")
        self.test_bulk_insert("2016_09_13_16_54_44")
        self.test_bulk_insert("2016_09_13_16_56_54")

        # check between 2016_09_13_16_53_24, 2016_09_13_16_53_34
        potential_files = self.tmp_api.get_by_time(1473803604, 1473803614)

        self.assertEqual(len(potential_files), 11)

    def test_get_by_time_not_consolidated(self):
        self.test_insert()
        self.test_bulk_insert()
        self.test_bulk_insert("2016_09_13_16_53_34")
        self.test_bulk_insert("2016_09_13_16_54_44")
        self.test_bulk_insert("2016_09_13_16_56_54")

        # check between 2016_09_13_16_53_24, 2016_09_13_16_53_34
        self.tmp_api.consolidate = False
        potential_files = self.tmp_api.get_by_time(1473803604, 1473803614)

        self.assertEqual(len(potential_files), 21)

    def test_insert(self):
        start_time = "2016_09_13_16_53_24"
        label = "port_scan"
        full_path = "../data/test_one_filtered.h5"

        self.tmp_api.insert(start_time, label, full_path, [0])
        results = self.tmp_api.get_by_label(label)

        self.assertGreaterEqual(len(results), 1)
        self.assertIsInstance(results[0][0], str)
        self.assertIsInstance(results[0][1], set)
        self.assertIsInstance(list(results[0][1])[0], int)

    def test_bulk_insert(self, start_time=None, label=None):
        label = label or "base_line"

        search_glob = "./data/*.h5"
        file_names = glob.glob(search_glob)
        start_time = start_time or "2016_09_13_16_53_24"
        tmp_file_dict = dict()
        for file_name in file_names:
            try:
                tmp_file_dict[start_time].append(file_name)
            except KeyError:
                tmp_file_dict[start_time] = [file_name]

        self.tmp_api.bulk_insert(tmp_file_dict, label)

        results = self.tmp_api.get_by_label(label)
        self.assertGreaterEqual(len(results), len(file_names))
        self.assertIsInstance(results[0][0], str)
        self.assertIsInstance(results[0][1], set)
        self.assertIsInstance(list(results[0][1])[0], int)
