#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 28 16:14:08 2019

@author: sebastien@gardoll.fr
"""

import math
import logging
from enum_utils import CoordinateFormat

class CoordinateUtils:

  _CONVERT_MAPPING = dict()

  @staticmethod
  def round_nearest(value, resolution, num_decimal):
    return round(round(value / resolution) * resolution, num_decimal)

  @staticmethod
  def get_convert_mapping(from_format, to_format, resolution):
    if from_format in CoordinateUtils._CONVERT_MAPPING:
      from_format_dict = CoordinateUtils._CONVERT_MAPPING[from_format]
      if to_format in from_format_dict:
        to_format_dict = from_format_dict[to_format]
        if resolution in to_format_dict:
          return to_format_dict[resolution]
        else:
          return CoordinateUtils._compute_mapping(from_format, to_format,
                                                  resolution, to_format_dict)
      else:
        to_format_dict = dict()
        from_format_dict[to_format] = to_format_dict
        return CoordinateUtils._compute_mapping(from_format, to_format,
                                                resolution, to_format_dict)
    else:
      from_format_dict = dict()
      CoordinateUtils._CONVERT_MAPPING[from_format] = from_format_dict
      to_format_dict = dict()
      from_format_dict[to_format] = to_format_dict
      return CoordinateUtils._compute_mapping(from_format, to_format,
                                              resolution, to_format_dict)

  @staticmethod
  def _compute_mapping(from_format, to_format, resolution, parent_mapping):
    try:
      result = CoordinateUtils._GENERATOR[from_format][to_format](resolution)
      parent_mapping[resolution] = result
      return result
    except:
      msg = f"the conversion of coordinates from '{from_format}' to '{to_format}' is not supported"
      logging.error(msg)
      raise Exception(msg)

  @staticmethod
  def _generate_mapping_degrees_east_european_to_american(resolution):
    dest_values = CoordinateUtils._generate_float_range(0, 360, resolution)
    keys_1 = CoordinateUtils._generate_float_range(0, (180+resolution), resolution)
    keys_2 = CoordinateUtils._generate_float_range((-180+resolution), 0, resolution)
    result = dict()
    index = 0
    for key in keys_1:
      result[key] = dest_values[index]
      index = index + 1
    for key in keys_2:
      result[key] = dest_values[index]
      index = index + 1
    return result

  @staticmethod
  def _generate_float_range(start, stop, resolution):
    if resolution < 1:
      nb_decimal = len(str(resolution)) - 2 # -2 for the zero and the dot.
      factor = math.pow(10, nb_decimal)
      int_range = range(int(start*factor), int(stop*factor), int(resolution*factor))
      result = list()
      for value in int_range:
        result.append(value/100)
      return result
    else:
      return range(start, stop, resolution)

  _GENERATOR = {CoordinateFormat.M_180_TO_180_DEGREE_EAST: {
    CoordinateFormat.ZERO_TO_360_DEGREE_EAST: _generate_mapping_degrees_east_european_to_american.__func__}}

"""
from coordinate_utils import unit_test
unit_test()
"""
def unit_test():
  mapping = CoordinateUtils.get_convert_mapping(CoordinateFormat.M_180_TO_180_DEGREE_EAST,
                                           CoordinateFormat.ZERO_TO_360_DEGREE_EAST,
                                           0.25 )
  return mapping