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
import copy
import logging as lg
import time

from adlkit.data_provider.data_providers import H5FileDataProvider

lg.basicConfig(level=lg.INFO)


def generator_output(batch_size=2048, end_count=100, n_readers=20,
                     q_multiplier=3, read_multiplier=1):
    from adlkit.data_provider.tests.mock_config import mock_sample_specification
    mock_sample_specification = copy.deepcopy(mock_sample_specification)
    tmp_data_provider = H5FileDataProvider(mock_sample_specification,
                                           batch_size=batch_size,
                                           n_readers=n_readers,
                                           q_multipler=q_multiplier,
                                           wrap_examples=True,
                                           read_multiplier=read_multiplier,
                                           make_file_index=True)

    tmp_data_provider.start()
    count = 0

    # spool up time
    for _ in range(10):
        this = tmp_data_provider.first().generate().next()

    bench_start_time = time.time()
    while count < end_count:
        count += 1
        tmp_data_provider.first().generate().next()

    delta = time.time() - bench_start_time
    tmp_data_provider.hard_stop()
    print_results('generator_output', delta, count, batch_size, n_readers,
                  q_multiplier, read_multiplier)


def print_results(name, delta, count, batch_size, n_readers, q_multiplier,
                  read_multiplier):
    print('**{0}**'.format(name))
    print('delta', delta)
    print('delivered', count)
    print('avg_delta', delta / count)
    print('batch_size', batch_size)
    print('n_readers', n_readers)
    print('q_multiplier', q_multiplier)
    print('read_multiplier', read_multiplier)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='benchmarking tool')
    parser.add_argument('--loglevel', type=str, default='warning')
    parser.add_argument('--n_readers', type=int, default=4)
    parser.add_argument('--q_multiplier', type=int, default=3)
    parser.add_argument('--read_multiplier', type=int, default=1)

    args = parser.parse_args()

    if args.loglevel == 'info':
        level = lg.INFO
    elif args.loglevel == 'debug':
        level = lg.DEBUG
    elif args.loglevel == 'warning':
        level = lg.WARNING
    else:
        level = lg.WARNING

    lg.basicConfig(level=level)
    generator_output(n_readers=args.n_readers, read_multiplier=args.read_multiplier)
