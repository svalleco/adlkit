import os
import shelve

from adlkit.data_catalog.old_abstract_data_catalog import DataAPI
from adlkit.data_catalog.utils import epoch_time_to_file_name, file_name_to_epoch_time


class FileDataAPI(DataAPI):
    base_dir = str()
    label_dir = str()

    def __init__(self, base_dir, label_dir=None, consolidate=True):
        """

        :param base_dir: '/some/top/level/path'
        :param label_dir: 'labels'
        """
        super(DataAPI, self).__init__()
        self.base_dir = os.path.abspath(base_dir)
        self.consolidate = consolidate
        self.label_dir = os.path.join(self.base_dir, label_dir or 'labels')

    def insert_by_label(self, label):
        # see FileDataAPI.insert
        raise NotImplemented

    def get_by_label(self, label):
        """

        :param label: directory name compliant name of a label to search for
        down the label_dir path
        :return: a list of tuples like the following where the first index is
        the file and the second index is the indices are the conversations
        that match the label
        <type 'tuple'>: ('./data/test_one_filtered.h5', [0, 1, 2])
        """
        label_path = os.path.join(self.label_dir, label)
        try:
            shelves = os.listdir(label_path)
        except OSError:
            return None

        shelve_paths = [os.path.join(label_path, shelve_path) for shelve_path
                        in shelves]

        return self.unpack_shelves(shelve_paths)

    def get_by_dict(self, search_dict):
        """
        :param search_dict: contains both a label and a time range
        {
          "range": {
            "epoch_start_time": {
              "gte": 1473803604,
              "lte": 1473803674
            }
          }
        }

        OR

        {"match": {
           "label_name": "label_one"
           }
        }


        :return:
        """

        # TODO allow both range and match via Label unions

        if "range" in search_dict:
            if "epoch_start_time" in search_dict["range"]:
                return self.get_by_time(search_dict['range']['epoch_start_time']['gte'],
                                        search_dict['range']['epoch_start_time']['lte'])
        elif "match" in search_dict:
            return self.get_by_label(search_dict['match']['label_name'])

    def get_by_time(self, start, end):
        """
        This expects ints and will attempt to convert strings to int,
        :param start: int(1473803814)
        :param end: int(1473803824)
        :return:
        """

        if isinstance(start, str):
            start = file_name_to_epoch_time(start)
        if isinstance(end, str):
            end = file_name_to_epoch_time(end)

        shelve_paths = list()
        for root, _, potential_files in os.walk(self.label_dir):
            for potential_file in potential_files:
                if not potential_file.endswith('.pickle.gz'):
                    continue
                potential_file_path = os.path.join(root, potential_file)
                if end >= file_name_to_epoch_time(potential_file) >= start:
                    # out.append((label, potential_file_path))

                    shelve_paths.append(potential_file_path)

        return self.unpack_shelves(shelve_paths)

    def generate_batch(self):
        raise NotImplemented

    def remove(self):
        raise NotImplemented

    def insert(self, str_start_time, label, file_path, indices):
        """

        :param str_start_time:
        :param label:
        :param file_path:
        :param indices:
        :return:
        """
        # https://docs.python.org/2/library/shelve.html
        # TODO start_time checks

        label_path = os.path.join(self.label_dir, label)

        try:
            os.mkdir(label_path)
        except OSError:
            pass

        if isinstance(str_start_time, int):
            str_start_time = epoch_time_to_file_name(str_start_time)
        # start_time = self.file_name_to_epoch_time(start_time)

        shelve_path = os.path.join(label_path, str_start_time + '.pickle.gz')
        shelve_handle = shelve.open(shelve_path, writeback=True)

        # TODO should I be using a set instead of a list?
        try:
            shelve_handle[file_path].update(indices)
        except KeyError:
            shelve_handle[file_path] = set(indices)

        shelve_handle.close()

    def bulk_insert(self, file_dict, label):
        # TODO
        # also accept tuples?
        """

        :param file_dict:
        :param label:
        :return:
        """
        for time_stamp, file_list in file_dict.items():
            for file_name in file_list:
                # TODO
                # determine a more clever method to list indices, -1
                # implies use all in file
                self.insert(time_stamp, label, file_name, [-1])

    def unpack_shelves(self, shelve_paths):
        results = list()
        for shelve_path in shelve_paths:
            results.extend(self.unpack_shelve(shelve_path))

        if self.consolidate:
            results = self.consolidate_results(results)

        return results

    @staticmethod
    def unpack_shelve(shelve_path):
        shelve_handle = shelve.open(shelve_path)
        tmp = shelve_handle.items()
        shelve_handle.close()
        return tmp

    @staticmethod
    def consolidate_results(shelve_tuples):
        tmp_file_paths_dict = dict()

        for file_path, indices in shelve_tuples:
            try:
                tmp_file_paths_dict[file_path].update(indices)
            except KeyError:
                tmp_file_paths_dict[file_path] = set(indices)

        return tmp_file_paths_dict.items()
