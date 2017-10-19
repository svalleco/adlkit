import logging as lg
from abc import ABCMeta, abstractmethod

import h5py


class DataIODriver(object):
    __metaclass__ = ABCMeta

    def __init__(self, opts=None):
        self.opts = opts or dict()

    def init(self):
        pass

    # TODO does it make sense to use the enter?
    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abstractmethod
    def get(self, *args):
        pass

    @abstractmethod
    def close(self, *args):
        pass


class H5DataIODriver(DataIODriver):
    file_handle_holder = None
    cache_handles = False

    def __init__(self, opts=None):
        super(H5DataIODriver, self).__init__(opts)
        self.file_handle_holder = dict()
        self.cache_handles = self.opts.get("cache_handles", False)

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        for handle in self.file_handle_holder.values():
            try:
                handle.close()
            except Exception as e:
                lg.error(e)

    def get(self, descriptor):
        if self.cache_handles:
            if descriptor in self.file_handle_holder:
                return self.file_handle_holder[descriptor]
            else:
                h5_file_handle = self.file_handle_holder[descriptor] = h5py.File(descriptor, 'r')
                return h5_file_handle
        else:
            return h5py.File(descriptor, 'r')

    def close(self, descriptor, handle):
        if not self.cache_handles:
            if descriptor not in self.file_handle_holder:
                handle.close()
            else:
                self.file_handle_holder[descriptor].close()
