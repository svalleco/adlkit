from __future__ import absolute_import

import copy

import numpy as np

FILLER_OFFSET = 1000
READER_OFFSET = 2000
WATCHER_OFFSET = 3000
GENERATOR_OFFSET = 4000

RANDOM = np.random.random

STOP_MESSAGE = 'stop'


class Config(dict):
    def __init__(self, a_dictionary):
        b_dictionary = copy.deepcopy(a_dictionary)
        super(Config, self).__init__(b_dictionary)

    def __getattr__(self, item):
        return self.get(item)

    def __setattr__(self, key, value):
        super(Config, self).__setitem__(key, value)


class ConfigurableObject(object):
    def __init__(self, default_config, config=None):
        self.defaults = Config(default_config)
        self.config = Config(default_config)

        if config:
            self.config.update(config)

        # TODO validators
        return

    def reset(self):
        self.config = Config(self.defaults)
