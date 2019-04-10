#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  4 16:16:56 2019

@author: sebastien@gardoll.fr
"""

import pandas as pd
import numpy as np
import logging
from coordinate_utils import CoordinateUtils
from enum_utils import DbFormat, CsvKey, TimeKey

class DbHandler:

  def __init__(self, dataset, label):
    self.dataset = dataset
    self.label = label

  @staticmethod
  def load(label):
    db_format = label.db_format
    try:
      static_method = DbHandler._LOAD_FORMAT_METHODS[db_format]
    except KeyError:
      msg = f"unsupported label db format '{db_format}'"
      logging.error(msg)
      raise Exception(msg)

    return static_method(label)

  @staticmethod
  def _load_csv_db(label):
    db_file_path = label.db_file_path
    db_format_options = label.db_format_options
    logging.info(f"opening label db '{db_file_path}'")
    with open(db_file_path, 'r') as db_file:
      try:
        separator          = db_format_options[CsvKey.SEPARATOR]
        header_line_number = db_format_options[CsvKey.HEADER]
        na_symbol          = db_format_options[CsvKey.NA_SYMBOL]
        dataset = pd.read_csv(db_file, sep=separator, header=header_line_number,
                              na_values=na_symbol)
      except KeyError:
        msg = 'missing csv option(s)'
        logging.error(msg)
        raise Exception(msg)
    return DbHandler(dataset, label)

  # See pandas'dataset.groupby().groups
  def get_group_mapping_by_time_resolution(self, variable_time_resolution):
    try:
      resolution_degree = TimeKey.KEYS.index(variable_time_resolution)
    except ValueError as e:
      msg = f"unknown '{variable_time_resolution}' mapping between time " \
        f"resolutions and time keys"
      logging.error(msg)
      raise Exception(e)
    list_keys = TimeKey.KEYS[0:(resolution_degree+1)]
    list_column_names = [self.label.db_meta_data_mapping[key] for key in list_keys]
    logging.debug(f"grouping dataset by '{list_column_names}'")
    return self.dataset.groupby(list_column_names).groups

  def round_coordinates(self, coordinate_key, resolution, nb_decimal):
    column_name = self.label.db_meta_data_mapping[coordinate_key]
    logging.info(
      f"round column '{column_name}' "
      f"in the db of label '{self.label.str_id}'")

    def _round_coordinates(value):
      rounded_value = CoordinateUtils.round_nearest(value, resolution, nb_decimal)
      return rounded_value

    self.dataset[column_name] = \
      np.vectorize(_round_coordinates)(self.dataset[column_name])

  def reformat_coordinates(self, coordinate_key, from_format, to_format,
                           resolution, nb_decimal):
    column_name = self.label.db_meta_data_mapping[coordinate_key]
    logging.info(f"reformat column '{column_name}' from format "
                 f"'from_format' to 'to_format' in the db of label"
                 f" '{self.label.str_id}'")
    coordinate_mapping = CoordinateUtils.get_convert_mapping(from_format,
                                                             to_format,
                                                             resolution)
    def _convert_coordinates(value):
      rounded_value = CoordinateUtils.round_nearest(value, resolution, nb_decimal)
      return coordinate_mapping[rounded_value]

    self.dataset[column_name] = \
      np.vectorize(_convert_coordinates) (self.dataset[column_name])

  _LOAD_FORMAT_METHODS = {DbFormat.CSV: _load_csv_db.__func__}