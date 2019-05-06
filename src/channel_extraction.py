#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  3 17:27:08 2019

@author: sebastien@gardoll.fr
"""

from extraction import ExtractionConfig
from db_handler import DbHandler
from enum_utils import CoordinateKey, CoordinatePropertyKey, TimeKey
import logging
from multiprocessing import Pool
import time_utils as tu
from time_series import XarrayTimeSeries
import xarray as xr
import pandas as pd
from tensor import Tensor
from os import path
from metadata_wrapper import MetadataWrapper

class ChannelExtraction:

  def __init__(self, extraction_config_path, variable_str_id):
    self.extraction_conf = ExtractionConfig.load(extraction_config_path)

    for variable in self.extraction_conf.get_variables():
      if variable.str_id == variable_str_id:
        self.extracted_variable = variable
        break

    if self.extracted_variable is None:
      msg = f"unknown variable '{variable_str_id}'"
      logging.fatal(msg)
      raise Exception(msg)

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

  def _format_label_dbs(self):
    var_format = self.extracted_variable.coordinate_metadata
    label_formats = dict()
    label_db_dict = dict()
    for label_db in self._label_dbs:
      curr_label = label_db.label
      label_formats[curr_label] = curr_label.coordinate_format
      label_db_dict[curr_label.str_id] = label_db

    for curr_label, curr_format in label_formats.items():
      for curr_key in CoordinateKey.KEYS:
        curr_var_format = var_format[curr_key][CoordinatePropertyKey.FORMAT]
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

  def _build_block_dict(self):
    group_mappings = list()
    netcdf_period_resolution = self.extracted_variable.netcdf_period_resolution
    for label_db in self._label_dbs:
      group_mapping = label_db.get_group_mapping_by_period(netcdf_period_resolution)
      group_mappings.append(group_mapping)

    set_group_keys = set()
    for group_mapping in group_mappings:
      set_group_keys.update(group_mapping.keys())

    block_dict = dict()
    block_index = 0
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
        block_dict[block_index] = curr_block
        block_index = block_index + 1
        curr_block = dict()
        nb_instantiated_block = nb_instantiated_block + 1

    if len(block_dict) < nb_instantiated_block:
      # Append the last instantiated block.
      block_dict[block_index] = curr_block
      block_index = block_index + 1

    return block_dict

  def _process_group_key(self, group_key, groups, buffer_list,
                         metadata_buffer_list):
    ts_time_dict = tu.from_time_list_to_dict(group_key)
    ts = XarrayTimeSeries(self.extracted_variable, ts_time_dict)
    for label_index in range(0, len(groups)):
      curr_buffer = buffer_list[label_index]
      curr_metadata_buffer = metadata_buffer_list[label_index]
      curr_db = self._label_dbs[label_index]
      for index in groups[label_index]:
        curr_time_dict, curr_lat, curr_lon = curr_db.get_location(index)
        subregion = ts.extract_square_region(self.extracted_variable,
                                             curr_time_dict,
                                             curr_lat, curr_lon,
                                             self.half_lat_frame,
                                             self.half_lon_frame)
        # Append to buffer.
        curr_buffer.append(subregion)
        location = list()
        label_num_id = self.extraction_conf.get_labels()[label_index].num_id
        location.extend((label_num_id, curr_lat, curr_lon))
        location.extend(curr_time_dict.values())
        curr_metadata_buffer.append(location)
    ts.close()

  def _process_block(self, block_item):
    block_num, block = block_item
    buffer_list = list()
    metadata_buffer_list = list()

    for label in self.extraction_conf.get_labels():
      buffer_list.append(list())
      metadata_buffer_list.append(list())

    # Extract the subregion.
    for group_key, groups in block.items():
      self._process_group_key(group_key, groups, buffer_list, metadata_buffer_list)

    # Save the buffers.

    # Coordinate formats in the tensor cannot be other than the variable'ones.
    # _checkformat ensures this.
    coordinate_format = dict()
    for key in CoordinateKey.KEYS:
      coordinate_format[key] = self.extracted_variable.coordinate_metadata[key][CoordinatePropertyKey.FORMAT]

    channel_id_to_index = {self.extracted_variable.str_id: 0}
    label_index = 0

    for label in self.extraction_conf.get_labels():
      data = xr.DataArray(buffer_list[label_index])

      column_names = self._compute_metadata_column_names(label)
      metadata = pd.DataFrame(data=metadata_buffer_list[label_index],
                              columns=column_names)

      block_filename, block_path = self._compute_block_names(block_num, label)
      is_channel_last = None

      block_tensor = Tensor(block_filename,
                            data, metadata, coordinate_format,
                            is_channel_last, channel_id_to_index)
      block_tensor.save(block_path)
      label_index = label_index + 1

  def _compute_metadata_column_names(self):
    # Use variable.time_resolution (db may have time_resolution greater then variable's one)
    result = ['label', 'lat', 'lon']
    time_resolution_degree = TimeKey.KEYS[self.extracted_variable.time_resolution]
    for index in range(0, time_resolution_degree + 1):
      key = TimeKey.KEYS[index]
      result.append(key)
    return result

  def _compute_block_names(self, block_num, label):
    extraction_id = self.extraction_conf.str_id
    label_display_name = label.display_name
    variable_id = self.extracted_variable.str_id

    block_filename = f"{extraction_id}_{variable_id}_{label_display_name}_block_{block_num}.{Tensor.FILENAME_EXTENSION}"
    block_path = path.join(self.extraction_conf.tmp_dir_path, block_filename)
    return (block_filename, block_path)


  def extract(self):
    # Match the format of the variable to be extracted and the format of the
    # label dbs.
    self._format_label_dbs()

    # Build the list of blocks to be processed.
    block_dict = self._build_block_dict()

    # Process the list of blocks.
    # Python 3.7 dict preserves order.
    with Pool(processes=self.extraction_conf.nb_process) as pool:
      pool.map(func=self._process_block, iterable=block_dict.items(), chunksize=1)

    # Merge the blocks and build a tensor object composed of 1 channel.
    # TODO: sperated method so as to implement failover
    # Compute the statistics on each label et the enter channel.
    # TODO: sperated method so as to implement failover

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
  variable_str_id = 'msl'
  driver = ChannelExtraction(extraction_config_path, variable_str_id)