import logging as lg
import os
import shelve
import shutil
from abc import ABCMeta

from .data_points import DataPoint, Label


class DataAPI(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

        # TODO remove this method
        # @abstractmethod
        # def insert(self, str_start_time, label, file_path, indices):
        #     pass
        #
        # @abstractmethod
        # def insert_by_data_point(self):
        #     pass
        #
        # @abstractmethod
        # def insert_label(self, label):
        #     pass
        #
        # @abstractmethod
        # def get_by_data_point_id(self, data_point_id):
        #     pass
        #
        # @abstractmethod
        # def get_by_label(self, label):
        #     pass
        #
        # @abstractmethod
        # def get_by_time(self, start, end):
        #     pass
        #
        # @abstractmethod
        # def get_by_dict(self, search_dict):
        #     pass
        #
        # @abstractmethod
        # def generate_batch(self):
        #     pass


class FileDataAPI(DataAPI):
    base_dir = str()
    label_dir = str()
    data_point_dir = str()
    all_label = None

    LABEL_TYPE = object()
    DATAPOINT_TYPE = object()

    def __init__(self, base_dir, label_dir=None, consolidate=True):
        """

        :param base_dir: '/some/top/level/path'
        :param label_dir: 'labels'
        """
        super(DataAPI, self).__init__()

        self.consolidate = consolidate

        self.base_dir = os.path.abspath(base_dir)
        self.label_dir = os.path.join(self.base_dir, label_dir or 'labels')
        self.data_point_dir = os.path.join(self.base_dir, 'data_points')

        self.directories = [self.base_dir, self.label_dir, self.data_point_dir]
        self._mkdirs()

        self.all_label = Label({
            'name': 'all',
        })

        self.save_label(self.all_label, upsert=False)

    def _mkdirs(self):
        for directory in self.directories:
            if not os.path.exists(directory):
                try:
                    os.mkdir(directory)
                except OSError as e:
                    lg.critical("Unable to use / make specified directory={0}".format(directory))
                    lg.error(e)
                    raise OSError

    def purge(self):
        shutil.rmtree(self.base_dir)
        self._mkdirs()

    def save_data_point(self, data_point, labels=None, upsert=True):
        assert issubclass(data_point.__class__, DataPoint)
        # shelve_path = os.path.join(self.data_point_dir, data_point.id + '.pickle.gz')

        # self.update_shelve(shelve_path, data_point.to_dict())

        shelve_handle, new = self._get_shelve(data_point)
        if shelve_handle is None or (not upsert and not new):
            return False

        else:
            self._update_shelve(shelve_handle, data_point.to_dict())

            write_labels = list(labels or []) + [self.all_label]

            for label in write_labels:
                label.append_data_point(data_point)
                # TODO determine a better way to check this.
                assert self.save_label(label) == True

            return True

    def get_labels(self):
        """
        This method should return a list of all Labels in the data base.
        :return: [Label]
        """
        try:
            labels = os.listdir(self.label_dir)
        except OSError:
            # TODO
            raise OSError

        for index, label in enumerate(labels):
            label_shelve_handle, _ = self._get_shelve(label, self.LABEL_TYPE)
            tmp = label_shelve_handle.items()

            labels[index] = Label(dict(tmp))
            label_shelve_handle.close()

        return labels

    def save_label(self, label, upsert=True):
        assert issubclass(label.__class__, Label)
        shelve_handle, new = self._get_shelve(label)

        if shelve_handle is None or (not upsert and not new):
            return False

        else:
            self._update_shelve(shelve_handle, label.to_dict())
            return True


    @staticmethod
    def _update_shelve(shelve_handle, tmp_dict):
        shelve_handle.update(tmp_dict)
        shelve_handle.close()

    def _get_shelve(self, item, shelve_type=None):
        """
        TODO I'm trying to do way too much with this function. Needs a solid revision.
        :param item:
        :param shelve_type:
        :return:
        """

        if isinstance(item, str) and shelve_type is not None:
            item_name = item
            if shelve_type == self.DATAPOINT_TYPE:
                shelve_path = os.path.join(self.data_point_dir, item_name)
            elif shelve_type == self.LABEL_TYPE:
                shelve_path = os.path.join(self.label_dir, item_name)
            else:
                lg.critical("BAD shelve_type, ABORTING")
                return None, None
        elif issubclass(item.__class__, Label):
            item_name = item.name
            shelve_path = os.path.join(self.label_dir, item_name)
        elif issubclass(item.__class__, DataPoint):
            item_name = item.id
            shelve_path = os.path.join(self.data_point_dir, item_name)
        else:
            lg.critical("BAD __class__ TYPE, ABORTING")
            return None, None

        if not shelve_path.endswith('.pickle.gz'):
            shelve_path = shelve_path + '.pickle.gz'

        new = not os.path.exists(shelve_path)
        return shelve.open(shelve_path, writeback=True), new
        #
        # def get_by_label(self, label):
        #     """
        #
        #     :param label: directory name compliant name of a label to search for
        #     down the label_dir path
        #     :return: a list of tuples like the following where the first index is
        #     the file and the second index is the indices are the conversations
        #     that match the label
        #     <type 'tuple'>: ('./data/test_one_filtered.h5', [0, 1, 2])
        #     """
        #     label_path = os.path.join(self.label_dir, label)
        #     try:
        #         shelves = os.listdir(label_path)
        #     except OSError:
        #         return None
        #
        #     shelve_paths = [os.path.join(label_path, shelve_path) for shelve_path
        #                     in shelves]
        #
        #     return self.unpack_shelves(shelve_paths)
        #
        # def get_by_dict(self, search_dict):
        #     """
        #     :param search_dict: contains both a label and a time range
        #     {
        #       "range": {
        #         "epoch_start_time": {
        #           "gte": 1473803604,
        #           "lte": 1473803674
        #         }
        #       }
        #     }
        #
        #     OR
        #
        #     {"match": {
        #        "label_name": "label_one"
        #        }
        #     }
        #
        #
        #     :return:
        #     """
        #
        #     # TODO allow both range and match via Label unions
        #
        #     if "range" in search_dict:
        #         if "epoch_start_time" in search_dict["range"]:
        #             return self.get_by_time(search_dict['range']['epoch_start_time']['gte'],
        #                                     search_dict['range']['epoch_start_time']['lte'])
        #     elif "match" in search_dict:
        #         return self.get_by_label(search_dict['match']['label_name'])
        #
        # def get_by_time(self, start, end):
        #     """
        #     This expects ints and will attempt to convert strings to int,
        #     :param start: int(1473803814)
        #     :param end: int(1473803824)
        #     :return:
        #     """
        #
        #     if isinstance(start, str):
        #         start = file_name_to_epoch_time(start)
        #     if isinstance(end, str):
        #         end = file_name_to_epoch_time(end)
        #
        #     shelve_paths = list()
        #     for root, _, potential_files in os.walk(self.label_dir):
        #         for potential_file in potential_files:
        #             if not potential_file.endswith('.pickle.gz'):
        #                 continue
        #             potential_file_path = os.path.join(root, potential_file)
        #             if end >= file_name_to_epoch_time(potential_file) >= start:
        #                 # out.append((label, potential_file_path))
        #
        #                 shelve_paths.append(potential_file_path)
        #
        #     return self.unpack_shelves(shelve_paths)
        #
        # def generate_batch(self):
        #     raise NotImplemented
        #
        # def remove(self):
        #     raise NotImplemented
        #
        # def insert(self, str_start_time, label, file_path, indices):
        #     """
        #
        #     :param str_start_time:
        #     :param label:
        #     :param file_path:
        #     :param indices:
        #     :return:
        #     """
        #     # https://docs.python.org/2/library/shelve.html
        #     # TODO start_time checks
        #
        #     label_path = os.path.join(self.label_dir, label)
        #
        #     try:
        #         os.mkdir(label_path)
        #     except OSError:
        #         pass
        #
        #     if isinstance(str_start_time, int):
        #         str_start_time = epoch_time_to_file_name(str_start_time)
        #     # start_time = self.file_name_to_epoch_time(start_time)
        #
        #     shelve_path = os.path.join(label_path, str_start_time + '.pickle.gz')
        #     shelve_handle = shelve.open(shelve_path, writeback=True)
        #
        #     # TODO should I be using a set instead of a list?
        #     try:
        #         shelve_handle[file_path].update(indices)
        #     except KeyError:
        #         shelve_handle[file_path] = set(indices)
        #
        #     shelve_handle.close()
        #
        # def bulk_insert(self, file_dict, label):
        #     # TODO
        #     # also accept tuples?
        #     """
        #
        #     :param file_dict:
        #     :param label:
        #     :return:
        #     """
        #     for time_stamp, file_list in file_dict.items():
        #         for file_name in file_list:
        #             # TODO
        #             # determine a more clever method to list indices, -1
        #             # implies use all in file
        #             self.insert(time_stamp, label, file_name, [-1])
        #
        # def pack_shelve(self, data_point, shelve_path):
        #     pass
        #
        # def unpack_shelves(self, shelve_paths):
        #     results = list()
        #     for shelve_path in shelve_paths:
        #         results.extend(self.unpack_shelve(shelve_path))
        #
        #     if self.consolidate:
        #         results = self.consolidate_results(results)
        #
        #     return results
        #
        # @staticmethod
        # def update_shelve(shelve_path, update_dict):
        #     data_point_shelve_handle = shelve.open(shelve_path, writeback=True)
        #     data_point_shelve_handle.update(update_dict)
        #     data_point_shelve_handle.close()
        #
        # @staticmethod
        # def unpack_shelve(shelve_path):
        #     shelve_handle = shelve.open(shelve_path)
        #     tmp = shelve_handle.items()
        #     shelve_handle.close()
        #     return tmp
        #
        # @staticmethod
        # def consolidate_results(shelve_tuples):
        #     tmp_file_paths_dict = dict()
        #
        #     for file_path, indices in shelve_tuples:
        #         try:
        #             tmp_file_paths_dict[file_path].update(indices)
        #         except KeyError:
        #             tmp_file_paths_dict[file_path] = set(indices)
        #
        #     return tmp_file_paths_dict.items()
