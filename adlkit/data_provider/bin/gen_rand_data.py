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

import os
import sys

import h5py
import numpy as np


def gen_rand_data(path, n_files, n_rows):
    # TODO auto import to tests
    data_set_range = ['tensor_1', 'tensor_2']

    for item in range(0, n_files):
        # '/'.join(os.getcwd().split('/')[:-1])

        file_name = os.path.join(path, 'test_file_{0}.h5'.format(item))

        with h5py.File(file_name, 'w') as h5_file_handle:
            for data_set in data_set_range:
                h5_file_handle.create_dataset(str(data_set), data=np.random.rand(n_rows, 5))


# TODO generate different types of test data
if __name__ == '__main__':
    # TODO argparse

    path = os.path.abspath(sys.argv[1])
    n_files = int(sys.argv[2])
    n_rows = int(sys.argv[3])

    gen_rand_data(path, n_files, n_rows)
