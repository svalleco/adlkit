"""
Used as a testcase to demonstrate extra processes being spawned and not terminated when using python multiprocessing
/ billiard. - wghilliard
"""

# lg.basicConfig(level=lg.DEBUG)
import copy

from adlkit.data_provider import BaseGenerator, FileDataProvider, H5Filler, H5Reader
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
tmp_data_provider.start(filler_class=H5Filler,
                        reader_class=H5Reader,
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
