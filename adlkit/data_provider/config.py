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

import copy

FILLER_OFFSET = 1000
READER_OFFSET = 2000
WATCHER_OFFSET = 3000
GENERATOR_OFFSET = 4000
WRITER_OFFSET = 5000


class Config(dict):
    def __init__(self, a_dictionary):
        b_dictionary = copy.deepcopy(a_dictionary)
        super(Config, self).__init__(b_dictionary)

    def __getattr__(self, item):
        return self.get(item)

    def __setattr__(self, key, value):
        super(Config, self).__setitem__(key, value)


class ConfigurableObject(object):
    def __init__(self, default_config, config=None):
        self.defaults = Config(default_config)
        self.config = Config(default_config)

        if config:
            self.config.update(config)

        # TODO validators
        return

    def reset(self):
        self.config = Config(self.defaults)
