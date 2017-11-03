# -*- coding: utf-8 -*-
"""
ADLKit
Copyright ©2017 AnomalousDL, Inc.  All rights reserved.

AnomalousDL, Inc. (ADL) licenses this file to you under the Academic and Research End User License Agreement (the
"License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at

  http://www.anomalousdl.com/licenses/ACADEMIC-LICENSE.txt

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL ADL BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE, either express
or implied.  See the License for the specific language governing permissions and limitations under the License.
"""

from __future__ import absolute_import

import logging as lg
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


def timestamp_to_epoch(timestamp, out_type=int):
    assert isinstance(timestamp, datetime)
    return out_type(time.mktime(timestamp.timetuple()))


# sourced from
# https://stackoverflow.com/questions/6999726/how-can-i-convert-a-datetime-object-to-milliseconds-since-epoch-unix
# -time-in-p
epoch = datetime.utcfromtimestamp(0)


def timestamp_to_epoch_ms(dt):
    return (dt - epoch).total_seconds() * 1000.0


def epoch_ms_to_timestamp(epoch_ms):
    return datetime.utcfromtimestamp(epoch_ms / 1000.0)


def file_name_to_timestamp(file_name):
    epoch_time = file_name_to_epoch_time(file_name)
    return epoch_ms_to_timestamp(epoch_time * 1000)


def timed(target_function):
    def wrapper(*args, **kwargs):
        start = time.time()
        out = target_function(*args, **kwargs)
        end = time.time()
        lg.info("func={0} delta={1}".format(target_function.__name__, end - start))

        return out

    return wrapper
