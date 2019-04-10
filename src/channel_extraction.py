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
    self.extraction_conf = ExtractionConfig.load(extraction_config_path)
    self.variable_index = variable_index
    self.extracted_variable = self.extraction_conf.get_variables()[variable_index]
    self._label_dbs = list()

    for label in self.extraction_conf.get_labels():
      current_db = DbHandler.load(label)
      self._label_dbs.append(current_db)

  def _check_format(self):
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

  def _build_cycle_list(self):
    group_mappings = list()
    variable_time_resolution = self.extracted_variable.time_resolution
    for label_db in self._label_dbs:
      group_mapping = label_db.get_group_mapping_by_time_resolution(variable_time_resolution)
      group_mappings.append(group_mapping)

    set_group_keys = set()
    for group_mapping in group_mappings:
      set_group_keys.update(group_mapping.keys())

    cycle_list = list()
    current_task_dict = dict()
    nb_dict = 1
    for group_key in set_group_keys:
      groups = list()
      for group_mapping in group_mappings:
        groups.append(group_mapping.get(group_key, None))
      current_task_dict[group_key] = groups
      if len(current_task_dict) > self.extraction_conf.batch_size:
        cycle_list.append(current_task_dict)
        current_task_dict = dict()
        nb_dict = nb_dict + 1

    if len(cycle_list) < nb_dict:
      # Append the last instantiated dictionary.
      cycle_list.append(current_task_dict)

    return cycle_list

  def _process_cycle(self, task_dict):
    # Compute mapping between buffer index and the indexes in the groups.
    # Compute the reverse mapping.
    # Instantiate the channel buffer.
    pass

  def extract(self):
    # Match the format of the variable to be extracted and the format of the
    # label dbs.
    self._check_format()

    # Build the list of tasks to be processed. Each task is a cycle.
    cycle_list = self._build_cycle_list()

    # Process the list of cycles.
    for cycle in cycle_list:
      self._process_cycle(cycle)

    # Merge the blocks and build a tensor object composed of 1 channel.

    # Compute the statistics on the channel.

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