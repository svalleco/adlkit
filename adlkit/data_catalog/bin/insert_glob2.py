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

import argparse
import datetime
import glob
import logging as lg
import os
import time

import h5py
# from adlkit.data_api.core import Label
# from adlkit.data_api.base import FileDataAPI
from adlkit.data_catalog.config import base_dir
from adlkit.data_catalog.data_points import DataPoint

from adlkit.data_catalog.file_data_catalog import FileDataCatalog
from adlkit.data_catalog.utils import file_name_to_timestamp


def setup_file_api():
    return FileDataCatalog(base_dir)


def h5_file_insert(glob_string, data_sets, api, labels=None):
    """
    This function assumes that the name of the file is a string representation
    of the capture time. This will not work for ANYTHING else.

    :param glob_string: './data/*.h5'
    :param label: 'tabby'
    :param api: FileDataAPI
    :return:
    """
    start_time = time.time()
    lg.debug('glob_string={0}'.format(glob_string))
    file_paths = glob.glob(glob_string)
    lg.debug('file_paths={0}'.format(file_paths))

    for file_path in file_paths:
        file_name = os.path.basename(file_path)
        file_path = os.path.abspath(file_path)

        # Here we attempt to determine if the file_name corresponds to the insertion date
        # else, we just assume that it happened now and create the timestamp accordingly.
        try:
            timestamp = file_name_to_timestamp(file_name)
        except ValueError:
            timestamp = datetime.datetime.now()

        possible_labels = labels or [file_path.split('/')[-2]]

        for index, possible_label_name in enumerate(possible_labels):
            possible_labels[index] = api.get_label(possible_label_name)

        data_points = list()
        with h5py.File(file_path) as h5_file_handle:
            if data_sets:
                n_data_points = len(h5_file_handle[data_sets[0]])
            else:
                n_data_points = h5_file_handle.items()[0].shape

            lg.debug("file_path={0} n_data_points={1}".format(file_path, n_data_points))
            for index in range(n_data_points):
                data_points.append(DataPoint({
                    'timestamp': timestamp,
                    'data_sets': data_sets,
                    'file_path': file_path,
                    'index'    : index
                }))

        api.save_data_points(data_points, labels=possible_labels)

        # tmp = {
        #     'name': label or possible_label_name,
        #     'file_path': file_path,
        #     'indices': [-1],
        #     'data_sets': data_sets,
        #
        # }
        # possible_label
        # api.insert_by_label

        # api.insert(epoch_time, family, label or possible_label, file_path, [-1])
    lg.info("delta={0} n_files_upserted={1}".format(time.time() - start_time,
                                                    len(file_paths)))

    return len(file_paths)


# def parse_labels()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('glob', type=str, help="Make sure to use `\"`(s)")
    # parser.add_argument('--label', type=str, default=None)
    parser.add_argument('--labels', type=str, default=None)
    parser.add_argument('--level', type=str, default='debug')
    parser.add_argument('--backend', type=str, default='file')
    # parser.add_argument('--family', type=str, default=None)
    parser.add_argument('--data_sets', type=str, default=None)
    parser.add_argument('--ftype', type=str, default='h5')
    args = parser.parse_args()

    if args.labels:
        labels = args.labels.split(',')
    else:
        labels = None

    if args.data_sets:
        data_sets = args.data_sets.split(',')
    else:
        data_sets = []

    level = None
    if args.level == 'info':
        level = lg.INFO
    elif args.level == 'warning':
        level = lg.WARNING
    elif args.level == 'debug':
        level = lg.DEBUG

    if args.backend == 'file':
        tmp_api = setup_file_api()
    else:
        tmp_api = setup_file_api()

    lg.basicConfig(level=level)

    if args.ftype == 'h5':
        lg.debug("using h5 insertion method")
        h5_file_insert(args.glob, data_sets, tmp_api, labels)
    else:
        lg.debug('specified `ftype` isn\' registered')
