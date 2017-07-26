import os
import sys

import numpy as np
import h5py


def gen_rand_data(path, n_files, n_rows):
    # TODO auto import to tests
    data_set_range = ['tensor_1', 'tensor_2']

    for item in range(0, n_files):
        # '/'.join(os.getcwd().split('/')[:-1])

        file_name = os.path.join(path, 'test_file_{0}.h5'.format(item))

        with h5py.File(file_name, 'w') as h5_file_handle:
            for data_set in data_set_range:
                h5_file_handle.create_dataset(str(data_set), data=np.random.rand(n_rows, 5))


# TODO generate different types of test data
if __name__ == '__main__':
    # TODO argparse

    path = os.path.abspath(sys.argv[1])
    n_files = int(sys.argv[2])
    n_rows = int(sys.argv[3])

    gen_rand_data(path, n_files, n_rows)
