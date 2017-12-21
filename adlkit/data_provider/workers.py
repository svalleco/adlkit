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

import ctypes
import logging as lg
import multiprocessing as mp
import os
import signal
import time

import billiard
import numpy as np
from future.utils import raise_with_traceback

worker_log = lg.getLogger('data_provider.workers.worker')

HELLO = b'hello'
OH_HAI = b'oh_hai'
EXIT = b'exit_pls'
PRUNE = b'kthxbai'
ERROR = b'ono'


def error_handler(self):
    def error_handler_wrapper(func):
        def new_method(*args, **kwargs):
            try:
                output = func(self, *args, **kwargs)
            except Exception as e:
                self.stop_check = True
                self.comm_driver.write('ctl', ERROR)
                with self.error.get_lock():
                    self.error.value = str(e)
                if os.getppid() != billiard.current_process().pid:
                    os.kill(os.getppid(), signal.SIGUSR1)

                raise_with_traceback(e)

            return output

        return new_method

    return error_handler_wrapper


class WorkerError(Exception):
    worker_id = None

    def __init__(self, message, worker_id):
        super(WorkerError, self).__init__(message)

        self.worker_id = worker_id


class Worker(billiard.Process):
    send_signals = False
    comm_driver = None

    max_batches = None
    batch_count = None

    def __init__(self,
                 worker_id,
                 comm_driver,
                 sleep_duration=1,
                 **kwargs):
        super(Worker, self).__init__()

        np.random.seed()
        self.comm_driver = comm_driver
        self.comm_driver.start()
        self.worker_id = worker_id

        self.stop = mp.Value('i', 0)
        self.error = mp.Value(ctypes.c_char_p)

        self.stop_check = False

        self.batch_count = 0
        self.file_handle_holder = dict()
        self.sleep_duration = sleep_duration
        self.run = error_handler(self)(type(self).run)

    def run(self, **kwargs):
        return

    def get_all_commands(self):
        while not self.stop_check:
            msg = self.comm_driver.read('ctl', block=False)
            if msg is None:
                pass
            elif msg in [ERROR, EXIT]:
                self.stop_check = True
                # TODO - wghilliard - this is super hacky
                self.comm_driver.write('ctl', EXIT)
            elif msg == PRUNE:
                self.stop_check = True
            else:
                print(msg)
            return

    def should_stop(self):
        self.get_all_commands()
        if self.max_batches is not None:
            self.stop_check = self.stop_check or (self.batch_count >= self.max_batches)

        return self.stop_check

    def seppuku(self):
        with self.stop.get_lock():
            self.stop.value = 1

    def debug(self, message):
        pass

    def sleep(self):
        if self.sleep_duration is not None:
            time.sleep(self.sleep_duration)
        if os.getppid() == 1:
            self.stop_check = True
