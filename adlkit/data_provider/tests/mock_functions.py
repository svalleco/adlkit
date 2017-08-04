from __future__ import absolute_import


def mock_filter_function(h5_file_handle, data_set_names):
    potential_data_points = h5_file_handle[data_set_names[0]].shape[0]

    start_index = potential_data_points / 2
    end_index = potential_data_points - 1
    step = 2

    return range(start_index, end_index, step)
