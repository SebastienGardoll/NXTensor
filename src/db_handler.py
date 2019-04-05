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
from enum_utils import DbFormat, CsvKey

class DbHandler:

  def __init__(self, dataset, meta_data_mapping):
    self.dataset = dataset
    self.meta_data_mapping = meta_data_mapping

  @staticmethod
  def load(db_file_path, db_format, db_meta_data_mapping, db_format_options):
    try:
      static_method = DbHandler._LOAD_FORMAT_METHODS[db_format]
    except KeyError:
      msg = f"unsupported label db format '{db_format}'"
      logging.error(msg)
      raise Exception(msg)

    return static_method(db_file_path, db_meta_data_mapping, db_format_options)

  @staticmethod
  def _load_csv_db(db_file_path, db_meta_data_mapping, db_format_options):
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
    return DbHandler(dataset, db_meta_data_mapping)


  def reformat_coordinates(self, coordinate_key, from_format, to_format,
                           resolution, nb_decimal):
    logging.info(f"reformat '{coordinate_key}' from format 'from_format' to 'to_format'")
    coordinate_mapping = CoordinateUtils.get_convert_mapping(from_format,
                                                             to_format,
                                                             resolution)
    def _convert_coordinates(value):
      rounded_value = CoordinateUtils.round_nearest(value, resolution, nb_decimal)
      return coordinate_mapping[rounded_value]

    self.dataset[coordinate_key] = \
                   np.vectorize(_convert_coordinates) (self.dataset[coordinate_key])

  _LOAD_FORMAT_METHODS = {DbFormat.CSV: _load_csv_db.__func__}