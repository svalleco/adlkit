from unittest import TestCase

from adlkit.data_catalog.file_data_catalog import BaseDataPoint, FileDataCatalog, Label
from adlkit.data_catalog.utils import epoch_ms_to_timestamp, timestamp_to_epoch_ms


class TestFileDataCatalog(TestCase):
    tmp_api = None
    tmp_label = None
    tmp_data_point = None

    # @classmethod
    # def setUpClass(cls):
    #     cls.tmp_api = FileDataCatalog('./tmp')
    #     cls.tmp_label = Label({'name': 'thing'})
    #     cls.tmp_data_point = BaseDataPoint({'glip': 'glop'})
    #
    # @classmethod
    # def tearDownClass(cls):
    #     cls.tmp_api.purge()

    def setUp(self):
        self.tmp_api = FileDataCatalog('./tmp')
        self.tmp_label = Label({'name': 'thing'})
        self.tmp_data_point = BaseDataPoint({'glip': 'glop'})

    def tearDown(self):
        self.tmp_api.purge()
        # self.tmp_api._mkdirs()

    def test_save_label(self):
        value = self.tmp_api.save_label(self.tmp_label)
        self.assertTrue(value)

    def test_save_label_no_upsert(self):
        self.tmp_api.save_label(self.tmp_label)
        value = self.tmp_api.save_label(self.tmp_label, upsert=False)
        self.assertFalse(value)

    def test_get_labels(self):
        self.tmp_api.save_label(self.tmp_label)
        labels = self.tmp_api.get_labels()

        # The `all` and `thing` label should be present
        self.assertGreaterEqual(len(labels), 2)

        for item in labels:
            self.assertIsInstance(item, Label)

    def test_save_data_point(self):
        result = self.tmp_api.save_data_point(self.tmp_data_point)
        self.assertTrue(result)

    def test_save_data_point_with_labels(self):
        self.tmp_api.save_label(self.tmp_label)
        result = self.tmp_api.save_data_point(self.tmp_data_point,
                                              labels=[self.tmp_label])
        self.assertTrue(result)

    def test_get_by_id(self):
        self.tmp_api.save_data_point(self.tmp_data_point)

        result = self.tmp_api.get_by_id(self.tmp_data_point.id)

        self.assertIsInstance(result, BaseDataPoint)

    def test_get_by_label(self):
        self.tmp_api.save_data_point(self.tmp_data_point)
        results = self.tmp_api.get_by_label(self.tmp_api.all_label)

        self.assertIsInstance(results, list)
        self.assertGreaterEqual(len(results), 1)
        for result in results:
            self.assertIsInstance(result, BaseDataPoint)

    def test_get_by_time(self):
        upper = 10
        lower = 0
        check = 5
        tmp_data_points = list()
        for index in range(upper + 1):
            tmp_data_point = BaseDataPoint({'glip': 'glop'})
            tmp_data_points.append(tmp_data_point)
            self.tmp_api.save_data_point(tmp_data_point)

        # Upper bounds check
        ##################################################
        start_timestamp = tmp_data_points[check].timestamp
        end_timestamp = tmp_data_points[upper].timestamp

        results = self.tmp_api.get_by_time(start_timestamp,
                                           end_timestamp)

        self.assertIsInstance(results, list)
        self.assertGreaterEqual(len(results), upper - check)
        for result in results:
            self.assertIsInstance(result, BaseDataPoint)

        # Lower bounds check
        ##################################################
        start_timestamp = tmp_data_points[lower].timestamp
        end_timestamp = tmp_data_points[check].timestamp

        results = self.tmp_api.get_by_time(start_timestamp,
                                           end_timestamp)

        self.assertIsInstance(results, list)
        self.assertGreaterEqual(len(results), check - lower)
        for result in results:
            self.assertIsInstance(result, BaseDataPoint)

    def test_get_before(self):
        upper = 10
        check = 5
        tmp_data_points = list()
        for index in range(upper + 1):
            tmp_data_point = BaseDataPoint({'glip': 'glop'})
            tmp_data_points.append(tmp_data_point)
            self.tmp_api.save_data_point(tmp_data_point)

        end_timestamp = tmp_data_points[check].timestamp

        results = self.tmp_api.get_before(end_timestamp)

        self.assertIsInstance(results, list)
        # self.assertGreaterEqual(len(results), upper - check)
        self.assertEqual(len(results), len(self.tmp_api.time_index) - check)
        for result in results:
            self.assertIsInstance(result, BaseDataPoint)

    def test_get_after(self):
        check = 5
        lower = 0
        tmp_data_points = list()
        for index in range(check + 1):
            tmp_data_point = BaseDataPoint({'glip': 'glop'})
            tmp_data_points.append(tmp_data_point)
            self.tmp_api.save_data_point(tmp_data_point)

        start_timestamp = tmp_data_points[lower].timestamp

        results = self.tmp_api.get_after(start_timestamp)

        self.assertIsInstance(results, list)
        self.assertGreaterEqual(len(results), check)
        for result in results:
            self.assertIsInstance(result, BaseDataPoint)

    def test_epoch_ms(self):
        init = self.tmp_data_point.timestamp
        epoch_ms = timestamp_to_epoch_ms(init)
        out = epoch_ms_to_timestamp(epoch_ms)

        self.assertEqual(init, out)

    def test_get_by_time_with_labels(self):
        upper = 10
        out = list()
        labels = [self.tmp_label, self.tmp_api.all_label]
        for _ in range(upper + 1):
            tmp = BaseDataPoint({'glip': 'glop'})
            out.append(tmp)
            self.tmp_api.save_data_point(tmp, labels=labels)

        results = self.tmp_api.get_by_time(out[0].timestamp, out[-1].timestamp, labels=labels)

        self.assertIsInstance(results, list)
        self.assertEqual(len(results), upper)
        for result in results:
            self.assertIsInstance(result, BaseDataPoint)
