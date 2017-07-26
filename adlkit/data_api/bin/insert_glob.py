import glob
import os
import argparse
import logging as lg
import time

from adlkit.data_api.core import Label
from adlkit.data_api.base import FileDataAPI
from adlkit.data_api.config import base_dir, label_dir
from adlkit.data_api.utils import file_name_to_epoch_time

def setup_file_api():
    try:
        os.mkdir(base_dir)
    except OSError:
        pass

    try:
        os.mkdir(os.path.join(base_dir, label_dir))
    except OSError:
        pass

    return FileDataAPI(base_dir, label_dir)


def main(glob_string, family, label, data_sets, api):
    """
    This function assumes that the name of the file is a string representation
    of the capture time. This will not work for ANYTHING else.

    :param glob_string: './data/*.h5'
    :param label: 'all'
    :param api: ExampleApi
    :return:
    """
    start_time = time.time()

    file_paths = glob.glob(glob_string)
    lg.debug('file_paths={0}'.format(file_paths))

    for file_path in file_paths:
        file_name = os.path.basename(file_path)

        # Here we attempt to determine if the file_name corresponds to the insertion date
        # else, we just assume that it happened now and create the timestamp accordingly.
        try:
            epoch_time = file_name_to_epoch_time(file_name)
        except ValueError:
            epoch_time = None

        possible_label = file_path.split('/')[-2]

        tmp_dict = {
            'label_name': label or possible_label,
            'label_family': family,
            'file_path': file_path,
            'indices': [-1],
            'epoch_start_time': epoch_time,
            'data_sets': data_sets,

        }
        api.insert_by_label(Label(tmp_dict))

        # api.insert(epoch_time, family, label or possible_label, file_path, [-1])
    lg.info("delta={0} n_files_upserted={1}".format(time.time() - start_time,
                                                    len(file_paths)))

    return len(file_paths)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('glob', type=str)
    parser.add_argument('--label', type=str, default=None)
    parser.add_argument('--labels', type=str, default=None)
    parser.add_argument('--level', type=str, default='debug')
    parser.add_argument('--backend', type=str, default='file')
    parser.add_argument('--family', type=str, default=None)
    parser.add_argument('--data_sets', type=str, default=None)
    args = parser.parse_args()

    if args.labels:
        labels = args.labels.split(',')

    if args.data_sets:
        data_sets = args.data_sets.split(',')
    else:
        data_sets = []

    level = None
    if args.level == 'info':
        level = lg.INFO
    elif args.level == 'warning':
        level = lg.WARNING
    elif args.level == 'debug':
        level = lg.DEBUG

    tmp_api = None
    if args.backend == 'file':
        tmp_api = setup_file_api()

    lg.basicConfig(level=level)

    main(args.glob, args.family, args.label, data_sets, tmp_api)
