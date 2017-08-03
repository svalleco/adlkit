import h5py


class H5Crawler(object):
    def crawl(self, file_path, data_set):
        with h5py.File(file_path) as h5_file_handle:
            for index in range(len(h5_file_handle[data_set])):
                yield index, h5_file_handle[data_set][index]
