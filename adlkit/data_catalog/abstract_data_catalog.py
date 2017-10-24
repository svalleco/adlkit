import logging as lg
from abc import ABCMeta, abstractmethod

dc_lg = lg.getLogger('data_catalog')


class CatalogObject(object):
    def __init__(self, init_dict=None, **kwargs):
        self.from_dict(init_dict or {})
        self.from_dict(kwargs)

    def from_dict(self, init):
        for key, value in init.items():
            setattr(self, key, value)


class DataPoint(CatalogObject):
    uid = None
    timestamp = None

    source_name = None
    source_type = None
    index = None

    data_set_list = None


class LabelInstance(CatalogObject):
    uid = None
    timestamp = None

    data_point_uid = None
    label_uid = None


class Label(CatalogObject):
    uid = None
    timestamp = None
    name = None
    comment = None
    is_origin = None


class AbstractDataCatalog(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    # @abstractmethod
    # def purge(self):
    #     pass
    #
    # @abstractmethod
    # def create_label(self, label):
    #     assert isinstance(label, (dict, Label))
    #
    # @abstractmethod
    # def create_label_instance(self, label_instance):
    #     assert isinstance(label_instance, (dict, LabelInstance))
    #
    # @abstractmethod
    # def create_data_point(self, data_point):
    #     assert isinstance(data_point, (dict, DataPoint))
    #
    # @abstractmethod
    # def create_data_points(self, data_points):
    #     assert isinstance(data_points, list)
    #
    # @abstractmethod
    # def delete_label(self, label):
    #     assert isinstance(label, (dict, Label, str, unicode))

    # @abstractmethod
    # def delete_data_point(self, data_point):
    #     assert isinstance(data_point, (dict, DataPoint, str, unicode))

    # @abstractmethod
    # def read_all_labels(self):
    #     pass
    #
    # @abstractmethod
    # def read_label(self, label_name):
    #     assert isinstance(label_name, (str, unicode))
    #
    # @abstractmethod
    # def read_by_label(self, label):
    #     assert isinstance(label, (str, unicode, Label))
    #
    # @abstractmethod
    # def read_by_uid(self, data_point_uid):
    #     assert isinstance(data_point_uid, (str, unicode))
    #
    # @abstractmethod
    # def read_by_uids(self, data_point_uids):
    #     assert isinstance(data_point_uids, list)
    #
    # @abstractmethod
    # def read_by_time(self, start_time, end_time):
    #     pass
    #
    # @abstractmethod
    # def read_before(self, end_time):
    #     pass
    #
    # @abstractmethod
    # def read_after(self, start_time):
    #     pass

    # @abstractmethod
    def generate_batch(self):
        raise NotImplemented
