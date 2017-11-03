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

import glob
import os
import random

import h5py
import numpy as np

from adlkit.data_provider import H5FileDataProvider


# TODO - wghilliard - refactor this


class GeneratorCacher(object):
    def __init__(self, generator, batchsize, max,
                 wrap=True,
                 delivery_function=None,
                 cache_filename=None,
                 delete_cache_file=True,
                 GeneratorClass=None):
        self.max = max
        self.Generator = generator
        self.Wrap = wrap
        self.delivery_function = delivery_function
        self.preloaded = False
        self.D = []
        self.cachefilename = cache_filename
        self.deletecachefile = delete_cache_file
        self.batchsize = batchsize
        self.GeneratorClass = GeneratorClass

    def __del__(self):
        if self.deletecachefile:
            print("Removing Cache File:", self.cachefilename)
            os.remove(self.cachefilename)

    def PreloadGenerator(self):
        Done = False
        while not Done:
            if not self.preloaded:
                self.D = []
                first = True
                i = 0
                batchN = 0
                gen = self.Generator

                for D in gen:
                    if first:
                        first = False
                        for j, T in enumerate(D):
                            T0 = np.zeros((int(self.max * self.batchsize),) + T.shape[1:])
                            T0[0:self.batchsize] = T
                            self.D.append(T0)

                    for j, T in enumerate(D):
                        try:
                            self.D[j][i:i + T.shape[0]] = T
                        except:
                            print("Something went wrong...")
                            print(i, j, T.shape, D[j].shape)

                    i += self.batchsize
                    batchN += 1
                    if batchN > self.max:
                        break

                    if self.delivery_function:
                        yield list(self.delivery_function(D))
                    else:
                        yield list(D)
                self.preloaded = True
            else:
                for i in range(0, self.max, self.batchsize):
                    out = []
                    for d in self.D:
                        if i + self.batchsize >= self.max:
                            remainder = self.max - i
                            endi = i + remainder
                        else:
                            endi = i + self.batchsize
                        out.append(d[i:endi])

                    D = tuple(out)
                    if self.delivery_function:
                        yield list(self.delivery_function(D))
                    else:
                        yield list(D)
            Done = not self.Wrap

    def DiskCacheGenerator(self, n_threads=4):
        Done = False
        secondpass = False
        renamecachefile = False
        if not self.preloaded:
            if self.cachefilename:
                self.deletecachefile = False
                if not os.path.exists(self.cachefilename):
                    renamecachefile = self.cachefilename.strip(".h5") + "-" + str(self.max) + ".h5"
                else:
                    renamecachefile = self.cachefilename
                if os.path.exists(renamecachefile):
                    self.cachefilename = renamecachefile
                    self.preloaded = True
                else:
                    found = False
                    files = glob.glob(self.cachefilename.strip(".h5") + "*.h5")
                    for file in files:
                        try:
                            N_in_File = int(file.split("-")[-1].strip(".h5"))
                            if self.max < N_in_File:
                                found = file
                                break
                        except:
                            pass
                    if found:
                        self.cachefilename = found
                    else:
                        self.cachefilename = self.cachefilename.strip(".h5") + "-" + str(
                                self.max) + "-PID" + str(os.getppid()) + ".h5"
            else:
                self.cachefilename = "/tmp/" + os.environ["USER"] + "-" + str(
                        os.getppid()) + "-" + str(int(10000 * random.random())) + ".h5"

        while not Done:
            if not self.preloaded:
                self.preloaded = True
                self.D = []
                first = True
                i = 0
                batchN = 0
                for D in self.Generator:
                    if first:
                        first = False
                        f = h5py.File(self.cachefilename, "w")
                        for j, T in enumerate(D):
                            T0 = f.create_dataset("dset" + str(j), (self.batchsize,) + T.shape[1:],
                                                  compression="lzf",
                                                  # chunks=(self.batchsize,)+ T.shape[1:],  # Something is wrong here!
                                                  maxshape=(None,) + T.shape[1:])
                            T0[0:self.batchsize] = T
                            self.D.append(T0)
                    else:
                        for j, T in enumerate(D):
                            # try:
                            self.D[j].resize(self.D[j].shape[0] + T.shape[0], axis=0)
                            self.D[j][i:i + T.shape[0]] = T
                            # except:
                            #    print "Something went wrong..."
                            #    print i, j, T.shape, D[j].shape

                    i += self.batchsize
                    batchN += 1
                    if batchN > self.max:
                        break

                    if self.delivery_function:
                        yield list(self.delivery_function(D))
                    else:
                        yield list(D)

                self.D = []
                f.close()
                try:
                    self.GeneratorClass.hard_stop()
                    pass
                except:
                    pass

                if renamecachefile:
                    if not os.path.exists(renamecachefile):
                        os.rename(self.cachefilename, renamecachefile)
                    else:
                        os.remove(self.cachefilename)
                    self.cachefilename = renamecachefile
            elif not secondpass:
                secondpass = True
                dsetnames = []

                for j in range(len(h5py.File(self.cachefilename, "r").keys())):
                    dsetnames.append("dset" + str(j))

                def PassThrough(payload):
                    out = []
                    for d in dsetnames:
                        out.append(payload[d])

                    return out

                genC = H5FileDataProvider([[self.cachefilename, dsetnames, "Foo", 1]],
                                          batch_size=self.batchsize,
                                          max=int(self.max / self.batchsize),
                                          process_function=PassThrough,
                                          delivery_function=self.delivery_function,
                                          n_readers=n_threads,
                                          q_multipler=1,
                                          n_buckets=1,
                                          read_multiplier=1,
                                          make_one_hot=False,
                                          sleep_duration=1,
                                          wrap_examples=self.Wrap)

                genC.start()
                gen = genC.first().generate()

                Done = not self.Wrap

                for D in gen:
                    yield list(D)
            else:
                Done = not self.Wrap

                for D in gen:
                    yield list(D)

            Done = not self.Wrap

    def PreloadData(self):
        gen = self.PreloadGenerator()

        MaxBatches = self.max / self.batchsize
        NBatches = 0

        if not self.preloaded:
            for D in gen:
                print
                ".",
                NBatches += 1
                if NBatches >= MaxBatches:
                    break
            print

    def CacheData(self):
        gen = self.DiskCacheGenerator()

        if not self.preloaded:
            for D in gen:
                print
                ".",
                pass
            print

#
# if __name__ == '__main__':
#     from CaloDNN.LoadData import *
#     import time
#
#     ECALShape = None, 25, 25, 25
#     HCALShape = None, 5, 5, 60
#
#     FileSearch = "/Users/afarbin/LCD/V1/*/*.h5"
#
#     Particles = ["ChPi", "Gamma", "Pi0", "Ele"]
#
#     MaxEvents = int(3.e6)
#     NTestSamples = 100000
#     NClasses = len(Particles)
#
#     BatchSize = 1024
#
#     NSamples = BatchSize * 10
#     ECAL = True
#     HCAL = True
#
#     ECALNorm = 'NonLinear'
#     HCALNorm = 'NonLinear'
#     multiplier = 2
#     n_threads = 3
#
#     TrainSampleList, TestSampleList, Norms, shapes = SetupData(FileSearch,
#                                                                ECAL,
#                                                                HCAL,
#                                                                False,
#                                                                NClasses,
#                                                                [float(NSamples) / MaxEvents,
#                                                                 float(NTestSamples) / MaxEvents],
#                                                                Particles,
#                                                                BatchSize,
#                                                                multiplier,
#                                                                ECALShape,
#                                                                HCALShape,
#                                                                ECALNorm,
#                                                                HCALNorm)
#
#     sample_spec_train = list()
#     for item in TrainSampleList:
#         sample_spec_train.append((item[0], item[1], item[2], 1))
#
#     q_multipler = 2
#     read_multiplier = 1
#     n_buckets = 1
#     from data_provider_core.data_providers import H5FileDataProvider
#
#     Train_genC = H5FileDataProvider(sample_spec_train,
#                                     batch_size=BatchSize,
#                                     max=int(NSamples / BatchSize),
#                                     process_function=LCDN(Norms),
#                                     # delivery_function=unpack,
#                                     n_readers=n_threads,
#                                     q_multipler=q_multipler,
#                                     n_buckets=n_buckets,
#                                     read_multiplier=multiplier,
#                                     make_one_hot=True,
#                                     sleep_duration=1,
#                                     wrap_examples=True)
#
#     print "Class Index Map:", Train_genC.config.class_index_map
#
#     print "Starting Training Generators...",
#     sys.stdout.flush()
#     Train_genC.start()
#     Train_gen = Train_genC.first().generate()
#     print "Done."
#
#     GC = GeneratorCacher(Train_gen, BatchSize, max=NSamples,
#                          wrap=True,
#                          delivery_function=None,
#                          cache_filename=None,
#                          delete_cache_file=True)
#
#     gen = GC.DiskCacheGenerator()
#
#     N = 1
#     count = 0
#     start = time.time()
#     for tries in xrange(2):
#         print "*********************Try:", tries
#         for D in gen:
#             Delta = (time.time() - start)
#             print count, ":", Delta, ":", Delta / float(N)
#             sys.stdout.flush()
#             N += 1
#             for d in D:
#                 print d.shape
#                 NN = d.shape[0]
#                 # print d[0]
#                 pass
#             count += NN
#
