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

import setuptools

from adlkit import __version__

setuptools.setup(
        name="adlkit",
        version=__version__,
        author="Amir Farbin, William Hilliard, Ryan Reece",
        author_email="wghilliard@anomalousdl.com",
        url="https://github.com/anomalousdl/adlkit",
        install_requires=[
            "keras==2.0.8",
            "numpy",
            "h5py",
            "theano",
            "billiard"
        ],
        packages=setuptools.find_packages()
)
