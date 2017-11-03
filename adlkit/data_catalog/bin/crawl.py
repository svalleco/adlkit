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
from collections import Counter

from adlkit.data_catalog import FileDataCatalog, Label
from adlkit.data_catalog.crawlers import H5Crawler
from adlkit.data_catalog.utils import file_name_to_timestamp


def setup_api():
    base_dir = os.getenv('BASE_DIR', '/tmp')

    return FileDataCatalog({
        'base_dir': base_dir
    })


def main(glob_string, label, data_sets):
    api = setup_api()
    counter = Counter()
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

    # TODO search by name or ID
    _, label_uid = api.save_label(Label({
        'name': label
    }))

    crawler = H5Crawler()

    for file_path in file_paths:
        file_name = os.path.basename(file_path)[:-3]

        # TODO don't hardcode this pls
        for index, value in crawler.crawl(file_path, 'ips'):
            host = value[0]
            peer = value[1]
            counter[file_path] += 1

            conversation_dict = {
                'start_time': file_name_to_timestamp(file_name),
                'end_time'  : file_name_to_timestamp(file_name) + datetime.timedelta(0, 10),

                'peer'      : repr(peer),
                'host'      : repr(host),

                'file_path' : file_path,
                'index'     : index,
                'data_sets' : data_sets,
            }

            _, conv_uid = api.insert_conversation(H5Conversation(conversation_dict))
            api.insert_instance(Instance({
                'conversation_uid': conv_uid,
                'label_uid'       : label_uid
            }))

    lg.info("delta={0} n_files_upserted={1}".format(time.time() - start_time,
                                                    len(file_paths)))
    lg.info(counter)
    return len(file_paths)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('glob', type=str)
    parser.add_argument('--label', type=str, default=None)
    # parser.add_argument('--labels', type=str, default=None)
    parser.add_argument('--level', type=str, default='debug')
    parser.add_argument('--data_sets', type=str, default=None)
    args = parser.parse_args()

    # TODO multiple label insertion
    # if args.labels:
    #     labels = args.labels.split(',')

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

    lg.basicConfig(level=level)

    main(args.glob, args.label, data_sets)
