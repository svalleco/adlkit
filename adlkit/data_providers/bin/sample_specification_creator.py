import glob
import os
import argparse
import pprint


def one_class(file_names, data_sets):
    sample_specification = []
    for file_name in file_names:
        sample_specification.append((
            os.path.abspath(file_name), data_sets, 'class_one', 1
        ))

    # print('sample_specification = {0}'.format(sample_specification))
    print "sample_specification = \\"
    pprint.pprint(sample_specification)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('glob', type=str)
    args = parser.parse_args()

    file_names = glob.glob(args.glob)[:10]
#    data_sets = ['tensor_1', 'tensor_2']
    import h5py
    with h5py.File('file_name') as h5_file_handle:
        h5_file_handle.keys()
    data_sets = ['tensor_1', 'tensor_2']

    one_class(file_names, data_sets)
