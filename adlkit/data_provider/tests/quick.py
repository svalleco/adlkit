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

"""
Used as a testcase to demonstrate extra processes being spawned and not terminated when using python multiprocessing
/ billiard. - wghilliard
"""

# lg.basicConfig(level=lg.DEBUG)
import copy

from adlkit.data_provider import BaseGenerator, FileDataProvider, FileFiller, FileReader
from adlkit.data_provider.tests.mock_config import mock_sample_specification

mock_sample_specification = copy.deepcopy(mock_sample_specification)

batch_size = 5

tmp_data_provider = FileDataProvider(mock_sample_specification,
                                     batch_size=batch_size,
                                     read_multiplier=2,
                                     make_one_hot=True,
                                     make_class_index=True,
                                     wrap_examples=True,
                                     n_readers=6
                                     )
import time

start_time = time.time()
tmp_data_provider.start(filler_class=FileFiller,
                        reader_class=FileReader,
                        generator_class=BaseGenerator,
                        # watcher_class=BaseWatcher
                        )
generator_id = 0

for _ in range(100):
    tmp = tmp_data_provider.generators[generator_id].generate().next()
    # TODO better checks

import psutil, os


def kill_proc_tree(pid, including_parent=False):
    parent = psutil.Process(pid)
    print('children:')
    for child in parent.children(recursive=True):
        print(child)
        # child.kill()
        # if including_parent:
        #     parent.kill()


me = os.getpid()
print('me', me)
kill_proc_tree(me)

tmp_data_provider.hard_stop()

print('me', me)
kill_proc_tree(me)

# import billiard
# billiard.process._cleanup()
# print(billiard.process.active_children())
# we are checking the that readers are killing themselves
# by checking that the pid no longer exists
# https://stackoverflow.com/questions/568271/how-to-check-if-there-exists-a-process-with-a-given-pid-in-python
# for reader_process in tmp_data_provider.readers:
#     this = None
#     try:
#         os.kill(reader_process.pid, 9)
#         this = True
#     except OSError:
#         this = False
#     finally:
#         assert this is False
print('time', time.time() - start_time)
input("pause\n")
