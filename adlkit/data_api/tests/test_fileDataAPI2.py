from unittest import TestCase

from adlkit.data_api.data_apis import FileDataAPI
from adlkit.data_api.data_points import DataPoint, Label


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
