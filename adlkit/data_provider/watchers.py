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
import time

from .config import WATCHER_OFFSET
from .workers import Worker

watcher_logger = lg.getLogger('data_provider.workers.watcher')


class BaseWatcher(Worker):
    def __init__(self, worker_id, shared_memory_pointer,
                 proxy_comm_drivers, comm_driver, max_batches=None, **kwargs):
        super(BaseWatcher, self).__init__(worker_id + WATCHER_OFFSET, comm_driver=comm_driver, **kwargs)
        self.shared_memory_pointer = shared_memory_pointer
        self.proxy_comm_drivers = proxy_comm_drivers

        self.max_batches = max_batches
        self.n_generators = len(proxy_comm_drivers)
        self.watcher_id = self.worker_id - WATCHER_OFFSET

    def debug(self, message):
        if isinstance(message, list):
            message = " ".join(message)
        watcher_logger.debug(" watchr_id={0} ".format(self.worker_id) + message)

    def info(self, message):
        if isinstance(message, list):
            message = " ".join(message)
        watcher_logger.info(" watchr_id={0} watchr_batch_id={1} ".format(self.worker_id,
                                                                         self.batch_count) + message)

    def run(self, **kwargs):
        self.watch()

    def watch(self):
        # TODO - wghilliard - when pruning generators, batch dropping may occur
        # TODO - wghilliard - keep pace switch
        # TODO - wghilliard - time out for locks

        out_queue_get_wait_time = time.time()
        self.debug("watcher starting")
        while not self.should_stop():
            read_batch = self.comm_driver.read('out', block=False)
            if read_batch is not None:
                self.debug("out_queue_get_wait_time={0}".format(time.time()
                                                                - out_queue_get_wait_time))
                start_time = time.time()
                for proxy_comm_driver in self.proxy_comm_drivers:
                    proxy_comm_driver.write('out', read_batch)
                self.debug("multicast_put_wait_time={0} ".format(time.time() - start_time))
                self.batch_count += 1
                out_queue_get_wait_time = time.time()

            start_time = time.time()

            for bucket_index, bucket in enumerate(self.shared_memory_pointer):
                with bucket[0].get_lock() and bucket[2].get_lock() and bucket[3].get_lock():
                    self.debug('does {}={}={} (bucket_index={})'.format(bucket[2].value,
                                                                        bucket[3].value,
                                                                        self.n_generators,
                                                                        # reader_index,
                                                                        bucket_index))
                    if bucket[2].value == bucket[3].value == self.n_generators:
                        self.debug(
                                "resetting bucket bucket_id={}".format(bucket_index))
                        bucket[0].value = 0
                        bucket[2].value = 0
                        bucket[3].value = 0
            self.debug(" bucket_watch_time={0} ".format(time.time() - start_time))

            self.debug(" batch_count={}/{}".format(self.batch_count, self.max_batches))
            self.sleep()
