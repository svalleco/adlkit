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
import multiprocessing
import time

import billiard
import numpy as np

# from .config import STOP_MESSAGE

worker_log = lg.getLogger('data_provider.workers.worker')

HELLO = b'hello'
OH_HAI = b'oh_hai'
EXIT = b'exit_pls'
PRUNE = b'kthxbai'


class WorkerError(Exception):
    worker_id = None

    def __init__(self, message, worker_id):
        super(WorkerError, self).__init__(message)

        self.worker_id = worker_id


# def report_error(run_function):
#     def wrapper():
#         try:
#             run_function()
#         except WorkerError:

class Worker(billiard.Process):
    comm_driver = None

    def __init__(self, worker_id,
                 # controller_socket_str,
                 # sync_str,
                 # controller_queue_depth=1,
                 comm_driver,
                 sleep_duration=1,
                 **kwargs):
        super(Worker, self).__init__()

        np.random.seed()
        self.comm_driver = comm_driver
        self.comm_driver.start()
        # zmq_context = zmq.Context()
        # self.controller_socket = zmq_context.socket(zmq.SUB)
        # self.controller_socket.setsockopt(zmq.RCVHWM, controller_queue_depth)
        # self.controller_socket.setsockopt(zmq.SUBSCRIBE, str(worker_id))
        # self.controller_socket.connect(controller_socket_str)
        #
        # self.sync_client = zmq_context.socket(zmq.REQ)
        # self.sync_client.connect(sync_str)

        # self.control_queue = multiprocessing.Queue(maxsize=controller_queue_depth)
        self.worker_id = worker_id

        self.stop = multiprocessing.Value('i', 0)
        self.stop_check = False

        self.batch_count = 0
        self.file_handle_holder = dict()
        self.sleep_duration = sleep_duration

        self.synced = False

    def run(self, **kwargs):
        return

    # def send_command(self, payload, block=True):
    #     try:
    #         # self.control_queue.put(payload, block=block)
    #         self.controller_socket.send_pyobj(payload, flags=zmq.NOBLOCK)
    #         return True
    #     # except Queue.Full:
    #     except zmq.ZMQError:
    #         # self.sleep()
    #         # worker_log.debug(" *{0}* command queue full".format(self.worker_id))
    #         return False

    # def get_command(self, block=True):
    # try:
    #     # return self.control_queue.get(block=block)
    #     return self.comm_driver.read('ctl')
    # # except Queue.Empty:
    # except :
    #     # worker_log.debug(" *{0}* command queue empty".format(self.worker_id))
    #     # self.sleep()
    #     return None
    # return self.comm_driver.read('ctl')

    def get_all_commands(self):
        while not self.stop_check:
            # tmp = self.get_command(block=False)
            msg = self.comm_driver.read('ctl', block=False)
            if msg is None:
                pass
            elif msg == EXIT:
                # self.hard_stop.set()
                print('{} SHOULD STOP'.format(self.worker_id))
                self.stop_check = True
                # TODO - wghilliard - this is super hacky
                # self.comm_driver.write('ctl', EXIT, block=False)
                self.comm_driver.write('ctl', EXIT)
            elif msg == PRUNE:
                self.stop_check = True
            else:
                print(msg)
            return

    def should_stop(self):
        self.get_all_commands()
        return self.stop_check

    def seppuku(self):
        for file_handle in self.file_handle_holder:
            try:
                self.file_handle_holder[file_handle].close()
            except Exception as e:
                lg.debug(e)

        with self.stop.get_lock():
            self.stop.value = 1

    def debug(self, message):
        # if isinstance(message, list):
        #     message = " ".join(message)
        # lg.info('{0} {1}'.format(time.time(), message))

        pass

    def sleep(self):
        if self.sleep_duration is not None:
            time.sleep(self.sleep_duration)
