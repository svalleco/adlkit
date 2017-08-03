import copy
import shelve
import uuid
from abc import ABCMeta
from datetime import datetime

from .utils import timestamp_to_epoch_ms


class DataPoint(object):
    __metaclass__ = ABCMeta

    timestamp = None
    id = None
    _origin = None

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
        # tmp = "{0}_{1}".format(repr(timestamp_to_epoch_ms(self.timestamp)), str(uuid.uuid4()))

        self.epoch_ts_str = repr(timestamp_to_epoch_ms(self.timestamp))

        # tmp = repr(timestamp_to_epoch_ms(self.timestamp))
        tmp = str(uuid.uuid4())
        self.id = self.id or tmp

        self.full_id = "/".join([self.epoch_ts_str, self.id])

    def from_shelve(self, shelve_item):
        tmp = shelve_item.items()
        self.from_dict(dict(tmp))

    def from_dict(self, init_dict):
        self._origin = copy.deepcopy(init_dict)
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

    def get_members(self):
        # return map(lambda x: float(x), self.members)
        return self.members
