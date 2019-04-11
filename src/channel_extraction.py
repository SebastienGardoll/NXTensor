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
from multiprocessing import Pool
import time_utils as tu
from time_series import XarrayTimeSeries
import numpy as np

class ChannelExtraction:

  def __init__(self, extraction_config_path, variable_index):
    self.extraction_conf = ExtractionConfig.load(extraction_config_path)
    self.variable_index = variable_index
    self.extracted_variable = self.extraction_conf.get_variables()[variable_index]
    self.half_lat_frame = (self.extraction_conf.y_size *
            self.extracted_variable.coordinate_metadata[CoordinateKey.LAT]
            [CoordinatePropertyKey.RESOLUTION])/2
    self.half_lon_frame = (self.extraction_conf.x_size *
            self.extracted_variable.coordinate_metadata[CoordinateKey.LON]
            [CoordinatePropertyKey.RESOLUTION])/2
    self._label_dbs = list()
    for label in self.extraction_conf.get_labels():
      current_db = DbHandler.load(label)
      self._label_dbs.append(current_db)

  def _check_format(self):
    var_format = self.extracted_variable
    label_formats = dict()
    label_db_dict = dict()
    for label_db in self._label_dbs:
      curr_label = label_db.label
      label_formats[curr_label] = curr_label.coordinate_format
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

  def _build_block_list(self):
    group_mappings = list()
    variable_time_resolution = self.extracted_variable.time_resolution
    for label_db in self._label_dbs:
      group_mapping = label_db.get_group_mapping_by_time_resolution(variable_time_resolution)
      group_mappings.append(group_mapping)

    set_group_keys = set()
    for group_mapping in group_mappings:
      set_group_keys.update(group_mapping.keys())

    block_list = list()
    curr_block = dict()
    nb_instantiated_block = 1

    count_group_key_per_block = 0
    nb_group_key_per_block = int(len(set_group_keys)/self.extraction_conf.nb_block) +1

    for group_key in set_group_keys:
      count_group_key_per_block = count_group_key_per_block + 1
      groups = list()
      for group_mapping in group_mappings:
        groups.append(group_mapping.get(group_key, None))
      curr_block[group_key] = groups
      if count_group_key_per_block >= nb_group_key_per_block:
        block_list.append(curr_block)
        curr_block = dict()
        nb_instantiated_block = nb_instantiated_block + 1

    if len(block_list) < nb_instantiated_block:
      # Append the last instantiated block.
      block_list.append(curr_block)

    return block_list

  def _preprocess_block(self, block):
    return None, None, None # TODO handle empty group.


  # !!!!!!!!!!!!! utiliser tensor et tensor meta data !!!!!!!!!!!!!!!!!!!!!!!!
  def _process_block_item(self, group_key, groups, buffer_list,
                          grp_index_to_buffer_index_list):
    ts_time_dict = tu.from_time_list_to_dict(group_key)
    ts = XarrayTimeSeries(self.extracted_variable, ts_time_dict)
    for label_index in range(0, len(groups)):
      curr_buffer = buffer_list[label_index]
      curr_grp_index_to_buffer = grp_index_to_buffer_index_list[label_index]
      for index in groups[label_index]:
        # TODO : mapping index dataset to (time_dict, lat, lon) in db_handler
        curr_time_dict, curr_lat, curr_lon = None, None, None
        subregion = ts.extract_square_region(self.extracted_variable,
                                             curr_time_dict,
                                             curr_lat, curr_lon,
                                             self.half_lat_frame,
                                             self.half_lon_frame)
        # Copy to buffer.
        buffer_index = curr_grp_index_to_buffer[index]
        np.copyto(dst=curr_buffer[buffer_index], src=subregion, casting='no')
        location.extend
    ts.close()

  def _process_block(self, block):
    # Compute mapping between buffer index and the indexes in the groups.
    buffer_list, grp_index_to_buffer_index_list,\
    buffer_index_to_tensor_metadata_list = self._preprocess_block(block)

    # Extract the subregion.
    for group_key, groups in block.items():
      self._process_block_item(group_key, groups, buffer_list,
                               grp_index_to_buffer_index_list)

    # Save the buffers.
    # TODO

  def extract(self):
    # Match the format of the variable to be extracted and the format of the
    # label dbs.
    self._check_format()

    # Build the list of blocks to be processed.
    block_list = self._build_block_list()

    # Process the list of blocks.
    with Pool(processes=self.extraction_conf.nb_process) as pool:
      pool.map(func=self._process_block, iterable=block_list, chunksize=1)

    # Merge the blocks and build a tensor object composed of 1 channel.

    # Compute the statistics on each label et the enter channel.

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