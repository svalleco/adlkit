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
import glob
import logging as lg
import os
import time

from adlkit.data_catalog.config import base_dir, label_dir

from adlkit.data_catalog.old_abstract_data_catalog import Label
from adlkit.data_catalog.old_base_data_catalog import FileDataAPI
from adlkit.data_catalog.utils import file_name_to_epoch_time


def setup_file_api():
    try:
        os.mkdir(base_dir)
    except OSError:
        pass

    try:
        os.mkdir(os.path.join(base_dir, label_dir))
    except OSError:
        pass

    return FileDataAPI(base_dir, label_dir)


def main(glob_string, family, label, data_sets, api):
    """
    This function assumes that the name of the file is a string representation
    of the capture time. This will not work for ANYTHING else.

    :param glob_string: './data/*.h5'
    :param label: 'all'
    :param api: ExampleApi
    :return:
    """
    start_time = time.time()

    file_paths = glob.glob(glob_string)
    lg.debug('file_paths={0}'.format(file_paths))

    for file_path in file_paths:
        file_name = os.path.basename(file_path)

        # Here we attempt to determine if the file_name corresponds to the insertion date
        # else, we just assume that it happened now and create the timestamp accordingly.
        try:
            epoch_time = file_name_to_epoch_time(file_name)
        except ValueError:
            epoch_time = None

        possible_label = file_path.split('/')[-2]

        tmp_dict = {
            'label_name'      : label or possible_label,
            'label_family'    : family,
            'file_path'       : file_path,
            'indices'         : [-1],
            'epoch_start_time': epoch_time,
            'data_sets'       : data_sets,

        }
        api.insert_by_label(Label(tmp_dict))

        # api.insert(epoch_time, family, label or possible_label, file_path, [-1])
    lg.info("delta={0} n_files_upserted={1}".format(time.time() - start_time,
                                                    len(file_paths)))

    return len(file_paths)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('glob', type=str)
    parser.add_argument('--label', type=str, default=None)
    parser.add_argument('--labels', type=str, default=None)
    parser.add_argument('--level', type=str, default='debug')
    parser.add_argument('--backend', type=str, default='file')
    parser.add_argument('--family', type=str, default=None)
    parser.add_argument('--data_sets', type=str, default=None)
    args = parser.parse_args()

    if args.labels:
        labels = args.labels.split(',')

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

    tmp_api = None
    if args.backend == 'file':
        tmp_api = setup_file_api()

    lg.basicConfig(level=level)

    main(args.glob, args.family, args.label, data_sets, tmp_api)
