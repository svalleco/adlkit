import bisect
import copy
import datetime
import glob
import logging as lg
import os
import shelve
import shutil
import uuid
from abc import ABCMeta

from .abstract_data_catalog import AbstractDataCatalog
from .utils import timestamp_to_epoch_ms


class BaseDataPoint(object):
    # TODO - wghilliard
    # revise this class, are AbstractDataPoints even needed?
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

        # timestamp is the only required attribute, all other info is derived from it.
        self.timestamp = self.timestamp or datetime.datetime.utcnow()

        self.epoch_ts_str = repr(timestamp_to_epoch_ms(self.timestamp))

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


class Label(BaseDataPoint):
    name = None

    members = None
    start_time = None
    end_time = None

    def append_data_point(self, data_point):
        assert issubclass(data_point.__class__, BaseDataPoint)
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


class FileDataCatalog(AbstractDataCatalog):
    base_dir = str()
    label_dir = str()
    data_point_dir = str()
    all_label = None

    LABEL_TYPE = object()
    DATAPOINT_TYPE = object()

    time_index = None

    suffix = None

    def __init__(self, base_dir, label_dir=None, consolidate=True, suffix=None):
        """

        :param base_dir: '/some/top/level/path'
        :param label_dir: 'labels'
        """
        super(AbstractDataCatalog, self).__init__()

        self.suffix = suffix or ".pickle.gz"

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

        self.time_index = self._mk_time_index()

    def purge(self):
        shutil.rmtree(self.base_dir)

    def save_data_point(self, data_point, labels=None, upsert=True, update_index=True):
        assert issubclass(data_point.__class__, BaseDataPoint)
        lg.debug("saving DataPoint={0}".format(data_point.id))
        shelve_handle, new = self._get_shelve(data_point)
        if shelve_handle is None or (not upsert and not new):
            return False

        self._update_shelve(shelve_handle, data_point.to_dict())

        write_labels = list(labels or []) + [self.all_label]

        for label in write_labels:
            label.append_data_point(data_point)
            # TODO determine a better way to check this.
            assert self.save_label(label) == True

        if update_index:
            self._update_time_index()

        return True

    def save_data_points(self, data_points, labels=None, upsert=True, update_index=True):
        for data_point in data_points:
            self.save_data_point(data_point, labels, upsert, update_index=False)
        if update_index:
            self._update_time_index()

    def save_label(self, label, upsert=True):
        assert issubclass(label.__class__, Label)
        shelve_handle, new = self._get_shelve(label)

        if shelve_handle is None or (not upsert and not new):
            return False

        self._update_shelve(shelve_handle, label.to_dict())
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

            labels[index] = self._wrap_shelve(label_shelve_handle, Label)

            label_shelve_handle.close()

        return labels

    def get_label(self, label_name, upsert=True):
        label_handle, new = self._get_shelve(label_name, self.LABEL_TYPE)
        # TODO wat do when label does not exist?
        if not upsert and new:
            return None
        if new:
            label_handle['name'] = label_name
        return self._wrap_shelve(label_handle, Label)

    def get_by_label(self, label):
        assert issubclass(label.__class__, Label)
        shelve_handle, _ = self._get_shelve(label)
        label_object = self._wrap_shelve(shelve_handle, Label)

        out = list(label_object.members)
        for index, member in enumerate(out):
            out[index] = self.get_by_id(member)

        return out

    def get_by_id(self, data_point_id):
        """

        :param data_point_id: str
        :return:
        """
        shelve_handle, new = self._get_shelve(data_point_id, self.DATAPOINT_TYPE, upsert=False)
        if new:
            return None
        data_point_object = self._wrap_shelve(shelve_handle, BaseDataPoint)
        return data_point_object

    def get_by_ids(self, data_point_ids):
        for index, data_point_id in enumerate(data_point_ids):
            data_point_ids[index] = self.get_by_id(data_point_id)
        return data_point_ids

    def get_by_time(self, start_time, end_time, labels=None):
        self._sanity_check(start_time, end_time)

        start_index = self._search_time(start_time)
        end_index = self._search_time(end_time)

        # TODO possible off-by-one error here
        data_point_ids = sum(self.data_point_index[start_index:end_index], [])

        if labels:
            assert isinstance(labels, list)
            assert len(labels) > 0
            labels = list(labels)

            if isinstance(labels[0], Label):
                pass
            elif isinstance(labels[0], str):
                for index, label in labels:
                    labels[index] = self.get_label(label, upsert=False)

            members = set()

            for label in labels:
                members.update(label.get_members())

            data_point_ids = list(members.intersection(data_point_ids))

        return self.get_by_ids(data_point_ids)

    def get_before(self, end_time):
        self._sanity_check(end_time=end_time)
        end_index = self._search_time(end_time)
        data_point_ids = sum(self.data_point_index[:end_index], [])

        return self.get_by_ids(data_point_ids)

    def get_after(self, start_time):
        self._sanity_check(start_time=start_time)
        start_index = self._search_time(start_time)
        data_point_ids = sum(self.data_point_index[start_index:], [])

        return self.get_by_ids(data_point_ids)

    @staticmethod
    def _update_shelve(shelve_handle, tmp_dict):
        shelve_handle.update(tmp_dict)
        shelve_handle.close()

    def _get_shelve(self, item, shelve_type=None, upsert=True):
        """
        TODO I'm trying to do way too much with this function. Needs a solid revision.
        :param item:
        :param shelve_type:
        :return:
        """

        if isinstance(item, str) and shelve_type is not None:
            item_name = item
            if not item_name.endswith(self.suffix):
                item_name = item_name + self.suffix

            if shelve_type == self.DATAPOINT_TYPE:
                search_path = os.path.join(self.data_point_dir, "*", item_name)
                shelve_path = glob.glob(search_path)[0]
                # shelve_path = os.path.join(self.data_point_dir, item_name)
            elif shelve_type == self.LABEL_TYPE:
                shelve_path = os.path.join(self.label_dir, item_name)
            else:
                lg.critical("BAD shelve_type, ABORTING")
                return None, None
        elif issubclass(item.__class__, Label):
            item_name = item.name
            shelve_path = os.path.join(self.label_dir, item_name)
        elif issubclass(item.__class__, BaseDataPoint):
            item_name = item.id
            # shelve_path = os.path.join(self.data_point_dir, item_name)
            shelve_path = os.path.join(self.data_point_dir, item.epoch_ts_str, item_name)
            try:
                os.mkdir(os.path.join(self.data_point_dir, item.epoch_ts_str))
            except OSError as e:
                # lg.warning(e)
                pass

        else:
            lg.critical("BAD __class__ TYPE, ABORTING")
            return None, None

        if not shelve_path.endswith(self.suffix):
            shelve_path = shelve_path + self.suffix

        new = not os.path.exists(shelve_path)
        if not upsert and new:
            return None, False

        return shelve.open(shelve_path, writeback=True), new

    def _sanity_check(self, start_time=None, end_time=None):
        all_label = self.get_label(self.all_label)
        if start_time:
            latest = all_label.end_time
            if start_time > latest:
                lg.warning("no DataPoints after: start_time={0}".format(start_time))
                return False

        if end_time:
            earliest = all_label.start_time
            if end_time < earliest:
                lg.warning("no DataPoints before: end_time={0}".format(end_time))
                return False
        return True

    def _wrap_shelve(self, shelve_handle, wrapper_class):
        tmp = shelve_handle.items()
        shelve_handle.close()
        return wrapper_class(dict(tmp))

    def _search_time(self, timestamp):
        """

        :param timestamp:
        :return:
        """
        assert issubclass(timestamp.__class__, datetime.datetime)
        target = timestamp_to_epoch_ms(timestamp)

        hit = bisect.bisect(self.time_index, target)

        if not hit:
            return None
        return hit

    def _update_time_index(self):
        lg.debug("updating time_index")
        self.time_index, self.data_point_index = self._mk_time_index()

    def _mk_time_index(self):
        # tmp_file_names = os.listdir(self.data_point_dir)
        path = os.path.join(self.data_point_dir, "*")
        tmp_file_names = glob.glob(path)

        time_index = list()
        data_point_index = list()
        for file_name in tmp_file_names:
            path = os.path.join(file_name, "*" + self.suffix)
            data_points = [os.path.basename(item)[:-len(self.suffix)] for item in glob.glob(path)]
            epoch_ts = float(os.path.basename(file_name))

            time_index.append(epoch_ts)
            data_point_index.append(data_points)

        # return map(lambda x: float(x[:-10]), tmp_file_names)
        return time_index, data_point_index

    def _mkdirs(self):
        for directory in self.directories:
            if not os.path.exists(directory):
                try:
                    os.mkdir(directory)
                except OSError as e:
                    lg.critical("Unable to use / make specified directory={0}".format(directory))
                    lg.error(e)
                    raise OSError
