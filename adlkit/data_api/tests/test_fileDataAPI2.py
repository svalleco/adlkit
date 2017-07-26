import datetime
from unittest import TestCase

from adlkit.data_api.data_apis import FileDataAPI
from adlkit.data_api.data_points import DataPoint, Label
from adlkit.data_api.utils import epoch_ms_to_timestamp, time_stamp_to_epoch_ms


class TestFileDataAPI(TestCase):
    def setUp(self):
        self.tmp_api = FileDataAPI('./tmp')
        self.my_label = Label({'name': 'thing'})
        self.my_data_point = DataPoint({'glip': 'glop'})

    def test_save_label(self):
        value = self.tmp_api.save_label(self.my_label)
        self.assertTrue(value)

    def test_save_label_no_upsert(self):
        self.tmp_api.save_label(self.my_label)
        value = self.tmp_api.save_label(self.my_label, upsert=False)
        self.assertFalse(value)

    def test_get_labels(self):
        self.tmp_api.save_label(self.my_label)
        labels = self.tmp_api.get_labels()

        # The `all` and `thing` label should be present
        self.assertGreaterEqual(len(labels), 2)

        for item in labels:
            self.assertIsInstance(item, Label)

    def test_save_data_point(self):
        result = self.tmp_api.save_data_point(self.my_data_point)
        self.assertTrue(result)

    def test_save_data_point_with_labels(self):
        self.tmp_api.save_label(self.my_label)
        result = self.tmp_api.save_data_point(self.my_data_point,
                                              labels=[self.my_label])
        self.assertTrue(result)

    def test_get_by_id(self):
        self.tmp_api.save_data_point(self.my_data_point)

        result = self.tmp_api.get_by_id(self.my_data_point.id)

        self.assertIsInstance(result, DataPoint)

    def test_get_by_label(self):
        self.tmp_api.save_data_point(self.my_data_point)
        results = self.tmp_api.get_by_label(self.tmp_api.all_label)

        self.assertIsInstance(results, list)
        self.assertGreaterEqual(len(results), 1)
        for result in results:
            self.assertIsInstance(result, DataPoint)

    def test_get_by_time(self):
        start_time = datetime.datetime.fromtimestamp(0)
        end_time = datetime.datetime.now()

        results = self.tmp_api.get_by_time(start_time, end_time)

    #   TODO finish this

    def test_search_time(self):
        upper = 10
        lower = 0
        check = 5
        tmp_data_points = list()
        for index in range(upper + 1):
            tmp_data_point = DataPoint({'glip': 'glop'})
            tmp_data_points.append(tmp_data_point)
            self.tmp_api.save_data_point(tmp_data_point)

        # start = self.tmp_api.time_index[upper]
        # end = self.tmp_api.time_index[check]

        ##################################################
        start_timestamp = tmp_data_points[check].timestamp
        end_timestamp = tmp_data_points[upper].timestamp

        results = self.tmp_api.get_by_time(start_timestamp,
                                           end_timestamp)

        self.assertIsInstance(results, list)
        self.assertGreaterEqual(len(results), upper - check)
        for result in results:
            self.assertIsInstance(result, DataPoint)
        ##################################################
        start_timestamp = tmp_data_points[lower].timestamp
        end_timestamp = tmp_data_points[check].timestamp

        results = self.tmp_api.get_by_time(start_timestamp,
                                           end_timestamp)

        self.assertIsInstance(results, list)
        self.assertGreaterEqual(len(results), check - lower)
        for result in results:
            self.assertIsInstance(result, DataPoint)

    def test_get_before(self):
        upper = 10
        check = 5
        tmp_data_points = list()
        for index in range(upper + 1):
            tmp_data_point = DataPoint({'glip': 'glop'})
            tmp_data_points.append(tmp_data_point)
            self.tmp_api.save_data_point(tmp_data_point)

        end_timestamp = tmp_data_points[check].timestamp

        results = self.tmp_api.get_before(end_timestamp)

        self.assertIsInstance(results, list)
        # self.assertGreaterEqual(len(results), upper - check)
        self.assertEqual(len(results), len(self.tmp_api.time_index) - check)
        for result in results:
            self.assertIsInstance(result, DataPoint)

    def test_get_after(self):
        check = 5
        lower = 0
        tmp_data_points = list()
        for index in range(check + 1):
            tmp_data_point = DataPoint({'glip': 'glop'})
            tmp_data_points.append(tmp_data_point)
            self.tmp_api.save_data_point(tmp_data_point)

        start_timestamp = tmp_data_points[lower].timestamp

        results = self.tmp_api.get_after(start_timestamp)

        self.assertIsInstance(results, list)
        self.assertGreaterEqual(len(results), check)
        for result in results:
            self.assertIsInstance(result, DataPoint)

    def test_epoch_ms(self):
        init = self.my_data_point.timestamp
        epoch_ms = time_stamp_to_epoch_ms(init)
        out = epoch_ms_to_timestamp(epoch_ms)

        self.assertEqual(init, out)
