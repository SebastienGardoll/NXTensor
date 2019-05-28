#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  3 17:27:08 2019

@author: sebastien@gardoll.fr
"""

from config_extraction import ExtractionConfig
from db_handler import DbHandler
from enum_utils import CoordinateKey, CoordinatePropertyKey, TimeKey, TensorKey,\
                       DBMetadata
import logging
from multiprocessing import Pool
import time_utils as tu
from time_series import XarrayTimeSeries
import xarray as xr
import pandas as pd
from tensor import Tensor
import os
from os import path
from yaml_class import YamlSerializable
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

    # group_mappings is a list of group_mapping from the different dbs.
    # This list is ordered following the order of the list of _label_dbs as
    # if the index of _label_dbs was a numerical id of the labels.
    for label_db in self._label_dbs:
      group_mapping = label_db.get_group_mapping_by_period(netcdf_period_resolution)
      group_mappings.append(group_mapping)

    # See get_group_mapping_by_period.
    # set_group_keys is the set of tuples that represent a period of time
    # (like (year, month), e.g. (2000, 10)).
    set_group_keys = set()
    for group_mapping in group_mappings:
      set_group_keys.update(group_mapping.keys())

    block_dict = dict()
    block_index = 0
    curr_block = dict()
    nb_instantiated_block = 1

    count_group_key_per_block = 0
    nb_group_key_per_block = int(len(set_group_keys)/self.extraction_conf.nb_block) + 1

    # Create a dictionary where a key is the block numerical id (zero based)
    # and a value is that block.
    # A block is a dictionary where a key is a group_key (time period) and a
    # value is list of list of indexes of label dbs (groups). This list is ordered
    # following the label order (as if the index of the list was a numerical id).
    # the list of indexes may be None.
    # self.extraction_conf.nb_block gives the maximum number of blocks, so the
    # number of groups in a block is computed according to this number.
    for group_key in set_group_keys:
      count_group_key_per_block = count_group_key_per_block + 1
      groups = list()
      # Harvest the groups from the db of the labels, for the same group key.
      for group_mapping in group_mappings:
        groups.append(group_mapping.get(group_key, None))
      curr_block[group_key] = groups
      if count_group_key_per_block >= nb_group_key_per_block:
        block_dict[block_index] = curr_block
        block_index = block_index + 1
        curr_block = dict()
        nb_instantiated_block = nb_instantiated_block + 1
        count_group_key_per_block = 0

    if len(block_dict) < nb_instantiated_block:
      # Append the last instantiated block.
      block_dict[block_index] = curr_block

    return block_dict

  def _process_group_key(self, group_key, groups, subregion_list,
                         location_subregion_list):
    ts_time_dict = tu.from_time_list_to_dict(group_key)
    with XarrayTimeSeries(self.extracted_variable, ts_time_dict) as ts:
      # Remember that groups is a list ordered following the order of the
      # _label_dbs. So label_index points a specific label.
      for label_index in range(0, len(groups)):
        curr_buffer = subregion_list[label_index]
        curr_metadata_buffer = location_subregion_list[label_index]
        curr_db = self._label_dbs[label_index]
        # list_indexes is a list of indexes of the current label db for the
        # current time period (group_key).
        # indexes may be None.
        list_indexes = groups[label_index]
        if list_indexes is not None:
          for index in list_indexes:
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
            location.extend(tu.remove_2d_time_dict(curr_time_dict).values())
            curr_metadata_buffer.append(location)

  def _process_block(self, block_item):
    block_num, block = block_item

    # Allocate the buffers.
    subregion_list = list()
    location_subregion_list = list()
    for label in self.extraction_conf.get_labels():
      subregion_list.append(list())
      location_subregion_list.append(list())

    # Extract the subregions.
    for group_key, groups in block.items():
      self._process_group_key(group_key, groups, subregion_list, location_subregion_list)

    # Save the buffers.

    # Coordinate formats in the tensor cannot be other than the variable'ones.
    # _format_label_dbs asserts this.
    coordinate_format = dict()
    for key in CoordinateKey.KEYS:
      coordinate_format[key] = self.extracted_variable.coordinate_metadata[key][CoordinatePropertyKey.FORMAT]

    label_index = 0

    block_yaml_file_paths = list()

    for label in self.extraction_conf.get_labels():
      if subregion_list[label_index]:
        logging.info(f"storing subregions of label '{label.str_id}' into a DataArray")
        # dims are lost when instantiating a DataArray based on other DataArray objects.
        dims = (self.extracted_variable.str_id, Tensor.X_LABEL, Tensor.Y_LABEL)
        # Store the subregions in a xarray data array.
        data = xr.DataArray(subregion_list[label_index], dims=dims)
        for item in subregion_list[label_index]:
          item.close()

        # Store the location of the subregion in a pandas data frame.
        logging.debug(f"storing locations of label '{label.str_id}' into a DataArray")
        column_names = self._compute_metadata_column_names()
        metadata_dataframe = pd.DataFrame(data=location_subregion_list[label_index],
                                          columns=column_names)
        metadata = MetadataWrapper(dataframe=metadata_dataframe)

        # Create a instance of Tensor (not as a real tensor but a part (block) of a
        # channel of a tensor).
        block_yaml_filename, block_yaml_file_path = self._compute_block_names(block_num, label)
        is_channel_last = None

        with Tensor(block_yaml_filename,
                    data, metadata, coordinate_format,
                    is_channel_last) as channel_block:
          logging.info(f"> saving label block '{block_yaml_filename}' (PID: {os.getpid()})")
          channel_block.save(block_yaml_file_path)

        block_yaml_file_paths.append(block_yaml_file_path)
      label_index = label_index + 1

    return block_yaml_file_paths

  def _compute_metadata_column_names(self):
    # Use variable.time_resolution (db may have time_resolution greater then variable's one)
    result = [DBMetadata.LABEL, DBMetadata.LAT, DBMetadata.LON]

    # Add the date metadata.
    time_resolution_degree = TimeKey.KEYS.index(self.extracted_variable.time_resolution)
    for index in range(0, time_resolution_degree + 1):
      key = TimeKey.KEYS[index]
      result.append(key)

    return result

  def _compute_block_names(self, block_num, label):
    extraction_id = self.extraction_conf.str_id
    label_display_name = label.display_name
    variable_id = self.extracted_variable.str_id

    block_yaml_filename = f"{extraction_id}_{variable_id}_{label_display_name}_block_{block_num}.{YamlSerializable.YAML_FILENAME_EXT}"
    block_yaml_file_path = path.join(self.extraction_conf.tmp_dir_path, block_yaml_filename)
    return (block_yaml_filename, block_yaml_file_path)

  def _concat_blocks(self, block_yaml_file_paths):
    logging.debug(f"concatenating the block: {block_yaml_file_paths}")

    # Bootstrap the concatenation of the blocks.
    index = 0
    channel = Tensor.load(block_yaml_file_paths[index])
    index = index + 1

    while(index < len(block_yaml_file_paths)):
      with Tensor.load(block_yaml_file_paths[index]) as current_block:
        channel.append(current_block)
      index = index + 1

    # Reset channel data.
    channel.data_file_path     = None
    channel.metadata_file_path = None
    channel.str_id             = self.extracted_variable.str_id
    return channel

  def _compute_tensor_name(self):
    return f"{self.extraction_conf.str_id}_{self.extracted_variable.str_id}_{TensorKey.CHANNEL}.{Tensor.YAML_FILENAME_EXT}"

  def extract(self):
    # Match the format of the variable to be extracted and the format of the
    # label dbs.
    self._format_label_dbs()

    # Build the list of blocks to be processed.
    block_dict = self._build_block_dict()

    # Process the list of blocks.
    # Python 3.7 dict preserves order.
    #"""
    logging.info(f"> processing {len(block_dict.items())} blocks with {self.extraction_conf.nb_process} processes")
    with Pool(processes=self.extraction_conf.nb_process) as pool:
      tmp_block_file_paths = pool.map(func=self._process_block,
                                      iterable=block_dict.items(), chunksize=1)
    # Flatten list of list of path files.
    block_file_paths = list()
    for item in tmp_block_file_paths:
      block_file_paths.extend(item)
    #"""

    #DEBUG
    """
    block_file_paths = list()
    for item in block_dict.items():
      block_file_paths.extend(self._process_block(item))
    """

    # Merge the blocks and build a tensor object composed of 1 channel.
    # Separated method so as to implement failover, one day...
    with self._concat_blocks(block_file_paths) as channel:
      # Compute the statistics of the channel and standardize it.
      channel.standardize() # It may be optinally computed via program args.
      # Save the channel along side the locations and the statistics.
      tensor_yml_filename = self._compute_tensor_name()
      tensor_yml_file_path = path.join(self.extraction_conf.tensor_dir_path, tensor_yml_filename)
      channel.save(tensor_yml_file_path)
"""
import logging
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
handler.setFormatter(formatter)
if logger.hasHandlers():
  logger.handlers.clear()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

from channel_extraction import unit_test
unit_test('/home/sgardoll/cyclone/extraction_config')
"""
def unit_test(config_parent_path):
  from os import path
  extraction_config_path = path.join(config_parent_path, '2000_10_extraction_config.yml')
  variable_str_id = 'wsl10'
  driver = ChannelExtraction(extraction_config_path, variable_str_id)
  driver.extract()

# DEBUG
unit_test('/home/sgardoll/cyclone/extraction_config')