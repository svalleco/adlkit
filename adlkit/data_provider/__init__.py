"""
DataProvider
==================

Provides:
  1. A data structure with a python generator interface
  2. Dynamic manipulation and transformation of data upon read and delivery




"""
from __future__ import absolute_import

from .data_providers import FileDataProvider, H5FileDataProvider, BaseDataProvider
from .config import Config, ConfigurableObject
from .workers import Worker
from .fillers import BaseFiller, H5Filler
from .readers import BaseReader, H5Reader
from .generators import BaseGenerator
from .watchers import BaseWatcher

__all__ = ['config', 'data_providers', 'fillers', 'generators', 'readers', 'watchers', 'workers']
