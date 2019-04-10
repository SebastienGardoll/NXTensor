#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 10 11:59:10 2019

@author: sebastien@gardoll.fr
"""

from xarray_wrapper import XarrayWrapper
from enum_utils import CoordinateFormat, CoordinateKey

class TensorMetadata(XarrayWrapper):

  def __init__(self, str_id, data = None, data_file_path = None):
    super(XarrayWrapper).__init__(str_id, data, data_file_path)
    self.coordinate_format = {CoordinateKey.LAT: CoordinateFormat.UNKNOWN,
                              CoordinateKey.LON: CoordinateFormat.UNKNOWN}

