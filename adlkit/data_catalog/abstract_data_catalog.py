from abc import ABCMeta, abstractmethod


class AbstractDataCatalog(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    def purge(self):
        pass

    @abstractmethod
    def save_label(self, label):
        pass

    @abstractmethod
    def save_data_point(self, data_point):
        pass

    @abstractmethod
    def save_data_points(self, data_points):
        pass

    @abstractmethod
    def get_labels(self):
        pass

    @abstractmethod
    def get_label(self, label_name):
        pass

    @abstractmethod
    def get_by_label(self, label):
        pass

    @abstractmethod
    def get_by_id(self, data_point_id):
        """

        :param data_point_id: str
        :return:
        """
        pass

    @abstractmethod
    def get_by_ids(self, data_point_ids):
        pass

    @abstractmethod
    def get_by_time(self, start_time, end_time):
        pass

    @abstractmethod
    def get_before(self, end_time):
        pass

    @abstractmethod
    def get_after(self, start_time):
        pass

    # @abstractmethod
    def generate_batch(self):
        raise NotImplemented
