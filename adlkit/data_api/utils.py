from __future__ import absolute_import

import os
import time
from datetime import datetime


def file_name_to_epoch_time(file_name):
    """
    This function will separate out any file_name extensions
    :param file_name: any of the following
                             str('2016_09_13_16_56_54'),
                             str('2016_09_13_16_53_24.h5')
                             str('./data/2016_09_13_16_53_24.h5')

    :return: int(1473803814)
    """
    date_time = os.path.basename(file_name)
    date_time = date_time.split('.')[0]
    pattern = '%Y_%m_%d_%H_%M_%S'
    return int(time.mktime(time.strptime(date_time, pattern)))


def epoch_time_to_file_name(date_time):
    tmp = datetime.fromtimestamp(date_time)
    # this can return days / months as single digits sometimes,
    # zfill ensures that there are always 2 digits.
    return '{0}_{1}_{2}_{3}_{4}_{5}'.format(tmp.year,
                                            str(tmp.month).zfill(2),
                                            str(tmp.day).zfill(2),
                                            str(tmp.hour).zfill(2),
                                            str(tmp.minute).zfill(2),
                                            str(tmp.second).zfill(2))


def timestamp_to_epoch(timestamp):
    assert isinstance(timestamp, datetime)
    return int(time.mktime(timestamp.timetuple()))
