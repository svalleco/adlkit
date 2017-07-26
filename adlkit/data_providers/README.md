# Data Provider Core

[![build status](https://gitlab.anomalousdl.com/anomalousdl/data_provider_core/badges/master/build.svg)](https://gitlab.anomalousdl.com/anomalousdl/data_provider_core/commits/master)


## Introduction
This package is designed to provide data to the DNN models of DLKit. The
`FileDataProvider` class uses python `multiprocessing` to cache disk reads
via sub-process in order to deliver data quickly to the main process.


## Examples
(from bin/benchmarks.generator_output)
```python

# Instantiate the DataProvider with non-default variables
tmp_data_provider = H5FileDataProvider(mock_sample_specification,
                                       batch_size=batch_size,
                                       n_readers=n_readers,
                                       q_multipler=q_multiplier,
                                       wrap_examples=True,
                                       read_multiplier=read_multiplier)

# Allocate memory and start sub-processes
tmp_data_provider.start()
count = 0

for _ in range(10):
        thing = tmp_data_provider.first().generate().next()
```

## Config:
All DataProviders have a read only variable store accessed via the `.config`
attribute. The initial state of `.config` is empty but can be overridden
in sub-classes via `ConfigurableObject.__init__`. If keywords are passed in,
they will overwrite the defaults.

Default FileDataProvider `.config`:
```python
default_config = {
            'batch_size': 2048,

            # If initial indices should be skipped, set this to the
            # correspinding index.
            'skip': 0,

            # The max number of batches to deliver, per generator.
            'max': 1e12,

            # If examples can be reused.
            'wrap_examples': True,

            # The number of reader processes to spawn. Each takes about ~4
            # file descriptors, so if you run out => make this number smaller.
            'n_readers': 20,

            # How many generators will be reading the same data? Less than
            # one will cause an error. -1 might work...
            'n_generators': 1,

            # If it is more efficient to read multiple batches at a time,
            # increment this.
            'read_multiplier': 1,

            # Not Implemented
            'waittime': 0.005,

            # False Not Implemented
            'use_shared_memory': True,

            # How many buckets should each reader have? When tuning,
            # scale with read_multiplier.
            'n_buckets': 10,

            # Queue depth coefficient for scaling with number of readers.
            'q_multipler': 1,

            # Not Implemented
            'GeneratorTimeout': 10,

            # Not Implemented
            'SharedDataQueueSize': 1,

            # 'timing': False,

            # A filter_function will be given the first index of the
            # sample_specification and asked to produce a list of read_indices.
            # It can either be a single function or a dictionary of class_name:function pairs.
            'filter_function': None,

            # TODO convert to {'fun_name': function()}
            # A process_function will consume an OrderedDictionary and be
            # expected to produce a list of numpy arrays to store into the
            # shared memory.
            'process_function': None,

            # If you need to swap the order, downsample, or any
            # other last-minute operation, do that here. Given a list of
            # numpy tensors, return a list of numpy tensors.
            # **NOTE** this is done in the main process.
            'delivery_function': None,

            # If the batch should contain the class index tensor and the
            # one_hot tensors.
            'make_class_index': False,
            'make_one_hot': False,

            # Not implemented.
            'catch_signals': False,

            # This is a race condition catch. Probably not a good idea to
            # disable but may make sense in edge-cases.
            'wait_for_malloc': True
        }
```



## Component Breakdown

### DataProviders
The base requirement is that a `sample_specification` must be given to the
 `DataProvider` upon instantiation. A `sample_specification` may look
 different depending on the class of `DataProvider` you are using.
 An example of `sample_specfication` for an `H5FileDataProvider` can be
 found in `tests/mock_config.py`

```python
mock_sample_specification = [
    ['../data/test_file_0.h5', ['tensor_1', 'tensor_2'], 'class_1', 1],
    ['../data/test_file_1.h5', ['tensor_1', 'tensor_2'], 'class_2', 2],
    ['../data/test_file_9.h5', ['tensor_1', 'tensor_2'], 'class_10', 10]
]
```

In this case, the `sample_specification` describes the location of the data that will
 be read, the `h5_data_sets` to be read, the string name of the class that
 is represented by such a data point, and the probably of any particular
 data point in the data set appearing in a batch.


The `BaseDataProvider` class only contains three methods that must be
 implemented by any subclass. The provided subclasses utilize a filesystem
 to store and organize batches. For instance, the `H5FileDataProvider`
 uses H5 files and their corresponding `h5_data_sets` to compile batches
 and read them in an efficient manner.

There are 3 main components that make up the structure of a `DataProvider`,
all of which are derived from the `Worker` class.

#### Fillers
The first part of the pipeline is the `Filler`, who's job can range from opening files
 and determining what data points should be used in the next batch to
 acting as a proxy for some other logic system that has already determined
 what data points should be in the batch. The only requirement that the
 filler has, is that
 it must submit complete batches to the `DataProvider.in_queue`. A batch produced
 by the `H5Filler` has the following format, but may differ if using a
 different implementation of the `Filler`.

```python
mock_batch = [
    [('class_2', '../data/test_file_1.h5', ['tensor_1', 'tensor_2'], (0, 22)),
     ('class_1', '../data/test_file_0.h5', ['tensor_1', 'tensor_2'], (0, 9)),
     ('class_10', '../data/test_file_9.h5', ['tensor_1', 'tensor_2'], [0, 4, 5])]
             ]
```

The batch is a list of sub-lists that consist of tuples similar to our
 `sample_specification` where instead of the last index corresponding to
 the probability of a class, it corresponds to the range of indices to be read.
 This may appear in a tuple format or a list, where a tuple (aka a `read_request`)
 indicates that
 the entire range should be read and the list implies that only explicit
 indices should be read. The `H5Filler` may produce either, depending on
 if a `filter_function` is used to prune out examples.

#### Readers
The next stage involves reading the data, either from disk, the network,
 some data store, whatever. The `H5Reader` processes a batch at a time
 (like the example provided above) and reads the data into a shared memory
 location so that it may be accessed by the generator quickly. The `H5Reader`
 will then submit completed batches to the `DataProvider.out_queue` where it will be
 picked up by the `Generator`. The completed batch looks like the following.

 ```python
 mock_read_batch = (0, 0, ['tensor_1', 'tensor_2'])
 ```

The first index corresponds to the `worker` that has read the batch, the second
 to the `bucket_index` that the data has been placed in, and the third is the
 name of the data_sets / features present. The shared memory
 is a data structure with the following shape.

    (n_readers, n_buckets, n_data_sets, data_set_shape)

#### Watchers
A Watcher is used to multiplex the completed batches to multiple generators.
The watcher will release the buckets when every generator has successfully
read the data. Problems may arise if the generators are not consuming at
the same pace, in which case your slowest generator will set the pace for
everyone.

To activate the `Watcher`, simple include the `Watcher` class to use as
part of the `DataProvider.start` function.

#### Generators
The last stage of the pipeline is the `Generator`. The `Generator` is responsible
for processing requests from the `DataProvider.out_queue` or its respective
`DataProvider.multicast_queues[$GENERATOR_ID]`
to read the data from the shared memory and hand it to whatever is calling
the `generate` function. It uses multiprocessing locks to either update the
watcher counter or release buckets. Using `.next()` on the `generate` function
will step the generator and `yield` a batch.

Example:
```python
for _ in range(10):
    thing = tmp_data_provider.first().generate().next()
```
**OR**
```python
for thing in tmp_data_provider.first().generate()
```

**OR** to use an explicit generator:

```
for thing in tmp_data_provider.generators[$GENERATOR_ID].generate()
```



## Testing
To run tests:

```bash
$ pip install pytest
$ cd tests
$ pytest
```