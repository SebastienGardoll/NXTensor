#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  3 17:27:08 2019

@author: sebastien@gardoll.fr
"""

from extraction import ExtractionConfig
from db_handler import DbHandler
from enum_utils import CoordinateKey, CoordinatePropertyKey
import logging

class ChannelExtraction:

  def __init__(self, extraction_config_path, variable_index):
    self._extraction_conf = ExtractionConfig.load(extraction_config_path)
    self.variable_index = variable_index
    self.extracted_variable = self._extraction_conf.get_variables()[variable_index]
    self._label_dbs = list()

    for label in self._extraction_conf.get_labels():
      current_db = DbHandler.load(label)
      self._label_dbs.append(current_db)

  def check_format(self):
    var_format = dict()
    var_format[CoordinateKey.LAT] =\
      {CoordinatePropertyKey.FORMAT    : self.extracted_variable.lat_format,
       CoordinatePropertyKey.RESOLUTION: self.extracted_variable.lat_resolution,
       CoordinatePropertyKey.NB_DECIMAL: self.extracted_variable.nb_lat_decimal}

    var_format[CoordinateKey.LON] = \
      {CoordinatePropertyKey.FORMAT    : self.extracted_variable.lon_format,
       CoordinatePropertyKey.RESOLUTION: self.extracted_variable.lon_resolution,
       CoordinatePropertyKey.NB_DECIMAL: self.extracted_variable.nb_lon_decimal}

    label_formats = dict()
    label_db_dict = dict()
    for label_db in self._label_dbs:
      curr_label = label_db.label
      curr_format = dict()
      curr_format[CoordinateKey.LAT] = curr_label.lat_format
      curr_format[CoordinateKey.LON] = curr_label.lon_format
      label_formats[curr_label] = curr_format
      label_db_dict[curr_label.str_id] = label_db

    for curr_label, curr_format in label_formats.items():
      for curr_key in CoordinateKey.KEYS:
        curr_var_format = var_format[curr_key]
        curr_label_format = curr_format[curr_key]
        curr_db = label_db_dict[curr_label.str_id]
        curr_resolution = var_format[curr_key][CoordinatePropertyKey.RESOLUTION]
        curr_nb_decimal = var_format[curr_key][CoordinatePropertyKey.NB_DECIMAL]

        if curr_var_format != curr_label_format:
          curr_db.reformat_coordinates(curr_key, curr_label_format,
                                       curr_var_format, curr_resolution,
                                       curr_nb_decimal)
        else:
          curr_db.round_coordinates(curr_key, curr_resolution, curr_nb_decimal)


  def extract(self):

    # Match the format of the variable to be extracted and the format of the
    # label dbs.
    self.check_format()

    # Build the list of tasks to be processed.

    # Instantiate the channel buffer.

    # Process the list of tasks.

    # Merge the blocks and build a tensor object composed of 1 channel.

    # Compute the statistics on the channel.

    pass
"""
import logging
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

from channel_extraction import unit_test
unit_test()
"""
def unit_test():
  from os import path
  config_parent_path = '/home/sgardoll/cyclone/extraction_config'
  extraction_config_path = path.join(config_parent_path, '2kb_extraction_config.yml')
  variable_index = 0
  driver = ChannelExtraction(extraction_config_path, variable_index)