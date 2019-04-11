#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 10 11:59:10 2019

@author: sebastien@gardoll.fr
"""

from data_wrapper import DataWrapper
from enum_utils import CoordinateFormat, CoordinateKey

class TensorMetadata(DataWrapper):

  def __init__(self, str_id, data = None, data_file_path = None, shape = None):
    super().__init__(str_id, data, data_file_path, shape)
    self.coordinate_format = {CoordinateKey.LAT: CoordinateFormat.UNKNOWN,
                              CoordinateKey.LON: CoordinateFormat.UNKNOWN}

