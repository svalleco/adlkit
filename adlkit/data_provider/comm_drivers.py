import multiprocessing as mp
from abc import ABCMeta, abstractmethod

from future.utils import raise_with_traceback
from six.moves import queue

DEFAULT_HWM = 1000


class BaseCommDriver(object):
    __metaclass__ = ABCMeta

    comm_handles = None

    opts = None
    vals = None

    def __init__(self, conf_dict):
        assert isinstance(conf_dict, dict)
        """
        config_dict = {
            'ctl': 10
        }
        
        Where the key is the name of the handle, and the value are the args for the underlying mechanisms.
        
        """
        self.opts = {'conf_dict': conf_dict}
        self.comm_handles = dict()

    def __getitem__(self, item):
        try:
            return self.comm_handles[item]
        except KeyError:
            return None

    @abstractmethod
    def write(self, key, payload, block=True):
        pass

    @abstractmethod
    def read(self, key, block=True):
        pass

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass


class QueueCommDriver(BaseCommDriver):

    def __init__(self, conf_dict):
        super(QueueCommDriver, self).__init__(conf_dict)

        for key, value in conf_dict.items():
            if isinstance(value, int):
                self.comm_handles[key] = mp.Queue(maxsize=value)
            elif isinstance(value, mp.queues.Queue):
                self.comm_handles[key] = value

    def start(self):
        pass

    def stop(self):
        pass

    def write(self, key, payload, block=True):
        while True:
            try:
                self.comm_handles[key].put(payload, block=False)
                return True
            except queue.Full:
                if not block:
                    return False
            except (Exception, KeyError) as e:
                raise_with_traceback(e)

    def read(self, key, block=True):
        while True:
            try:
                payload = self.comm_handles[key].get(block=False)
                return payload
            except queue.Empty:
                if not block:
                    return None
            except (Exception, KeyError) as e:
                raise_with_traceback(e)

    def drain(self, keys=None):
        keys = keys or self.comm_handles.keys()
        for key in keys:
            check = True
            while check is not None:
                check = self.read(key, block=False)
