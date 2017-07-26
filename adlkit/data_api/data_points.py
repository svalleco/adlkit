import copy
import shelve
from abc import ABCMeta
from datetime import datetime

from .utils import time_stamp_to_epoch_ms


class DataPoint(object):
    __metaclass__ = ABCMeta

    timestamp = None
    id = None
    _source = None

    def __init__(self, init_item=None):
        """
        This class consumes a dictionary and sets attributes accordingly.
        :param init_item:
        """
        if isinstance(init_item, dict):
            self.from_dict(init_item)

        if isinstance(init_item, shelve.DbfilenameShelf):
            self.from_shelve(init_item)

        # a required attribute, all other info is derived from it.
        self.timestamp = self.timestamp or datetime.utcnow()
        # tmp = "{0}_{1}".format(str(timestamp_to_epoch(self.timestamp)),
        #                        str(uuid.uuid4()))

        # tmp = str(uuid.uuid4())
        tmp = repr(time_stamp_to_epoch_ms(self.timestamp))
        self.id = self.id or tmp

    def from_shelve(self, shelve_item):
        tmp = shelve_item.items()
        self.from_dict(dict(tmp))

    def from_dict(self, init_dict):
        self._source = copy.deepcopy(init_dict)
        for key, value in init_dict.items():
            setattr(self, key, value)

    def to_dict(self):
        return self.__dict__


class Label(DataPoint):
    name = None
    members = None
    start_time = None
    end_time = None

    def append_data_point(self, data_point):
        assert issubclass(data_point.__class__, DataPoint)
        if not self.members:
            self.members = list()

        if data_point.id not in set(self.members):
            self.members.append(data_point.id)

        if not self.start_time:
            self.start_time = data_point.timestamp
        else:
            self.start_time = min(self.start_time, data_point.timestamp)

        if not self.end_time:
            self.end_time = data_point.timestamp
        else:
            self.end_time = max(self.end_time, data_point.timestamp)
