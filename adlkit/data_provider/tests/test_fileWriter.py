import os
from unittest import TestCase

import numpy as np

from adlkit.data_provider.comm_drivers import QueueCommDriver
from adlkit.data_provider.io_drivers import H5DataIODriver
from adlkit.data_provider.writers import BaseWriter


class TestWriter(TestCase):
    def test_init(self):
        comm_driver = QueueCommDriver({
            'ctl': 10
        })

        shape = [5, 10, 80, 40]
        data_dst = 'hello.h5'
        try:
            os.remove(data_dst)
        except OSError:
            pass
        max_batches = shape[0]

        def test_data_src():
            for index, datum in enumerate(np.random.rand(*shape)):
                yield (str(index ** 2), datum)

        writer = BaseWriter(
                worker_id=1,
                max_batches=max_batches,
                comm_driver=comm_driver,
                data_src=test_data_src(),
                data_dst=data_dst,
                io_driver=H5DataIODriver())

        writer.write()

        tmp_io_driver = H5DataIODriver()

        with tmp_io_driver:
            tmp_handle = tmp_io_driver.get(data_dst)

            for key in tmp_handle.keys():
                self.assertEqual(tmp_handle[key].shape, tuple(shape[1:]))

        os.remove(data_dst)
