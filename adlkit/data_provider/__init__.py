"""
DataProvider
==================

Provides:
  1. A data structure with a python generator interface
  2. Dynamic manipulation and transformation of data upon read and delivery




"""
from __future__ import absolute_import

from .data_providers import FileDataProvider, H5FileDataProvider, AbstractDataProvider, WatchedH5FileDataProvider
from .config import Config, ConfigurableObject
from .workers import Worker
from .fillers import BaseFiller, FileFiller
from .readers import BaseReader, FileReader
from .generators import BaseGenerator
from .watchers import BaseWatcher
from .cached_data_providers import GeneratorCacher

__all__ = ['config', 'data_providers', 'fillers', 'generators', 'readers', 'watchers', 'workers']
