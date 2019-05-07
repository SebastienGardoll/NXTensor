#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 10 11:38:36 2019

@author: sebastien@gardoll.fr
"""

from yaml_class import YamlSerializable
import logging
import xarray as xr
import os.path as path
from metadata_wrapper import MetadataWrapper
import numpy as np
from abc import abstractmethod

class DataWrapper(YamlSerializable):

  _FILENAME_EXTENSION = 'nc'

  def __init__(self, str_id, data, metadata):
    super().__init__(str_id)

    self.data_file_path     = None
    self.metadata_file_path = None
    self.shape              = None
    self._data              = None
    self._metadata          = None

    self.set_data(data)
    self.set_metadata(metadata)

  def append(self, other, dim_name):
    new_data = xr.concat((self.get_data(), other.get_data()), dim=dim_name)
    new_metadata = self.get_metadata().append(other.get_metadata())
    self.set_data(new_data)
    self.set_metadata(new_metadata)

  @abstractmethod
  def __len__(self):
    pass

  # Return a new instance of Tensor with its data shuffled for the first dimension
  # (keep metadata consistent)
  def shuffle(self, permutations = None):
    data = self.get_data()
    metadata = self.get_metadata()

    len_data = len(self)
    len_metadata = len(metadata)

    if len_data != len_metadata:
      msg = f"incompatible size of data and metadata (resp: {len_data} and {len_metadata})"
      logging.error(msg)
      raise Exception(msg)

    if permutations is None:
      permutations = np.random.permutation(len_data)

    metadata.shuffle(permutations)
    core_array = data.data[permutations]
    new_data = xr.DataArray(core_array)
    self.set_data(new_data)

    return permutations

  def set_data(self, data):
    self._data = data
    self.shape = data.shape

  def get_data(self):
    return self._data

  def set_metadata(self, metadata):
    self._metadata = metadata

  def get_metadata(self):
    return self._metadata

  def close(self):
    try:
      self._data.close()
      self._metadata.close()
    except :
      pass

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    self.close()

  def save(self, yaml_file_path):
    logging.info(f"saving data properties to {yaml_file_path}")

    if self.get_data() is None:
      msg = "missing data"
      logging.error(msg)
      raise Exception(msg)

    if self.get_metadata() is None:
      msg = "missing metadata"
      logging.error(msg)
      raise Exception(msg)

    # Making self._data and self._metadata transient for yaml serialization.
    data = self.get_data()
    del self._data
    metadata = self.get_metadata()
    del self._metadata

    if self.data_file_path is None:
      self.data_file_path = DataWrapper._compute_data_from_yaml_file_path(yaml_file_path)

    if self.metadata_file_path is None:
      self.metadata_file_path = DataWrapper._compute_metadata_from_yaml_file_path(yaml_file_path)

    super().save(yaml_file_path)

    self.set_data(data)
    self.set_metadata(metadata)
    self._save_data()
    self.get_metadata().save(self.metadata_file_path)

  def _save_data(self):
    try:
      logging.debug(f"saving data to {self.data_file_path}")
      self.data.to_netcdf(self.data_file_path, mode = 'w')
    except Exception as e:
      logging.error(f"cannot save the data to '{self.data_file_path}': {str(e)}")
      raise e

  @staticmethod
  def _compute_data_from_yaml_file_path(yaml_file_path):
    parent_dir_path = path.dirname(yaml_file_path)
    data_file_path = path.join(parent_dir_path,
      f"{path.basename(path.splitext(yaml_file_path)[0])}.{DataWrapper._FILENAME_EXTENSION}")
    return data_file_path

  @staticmethod
  def _compute_metadata_from_yaml_file_path(yaml_file_path):
    parent_dir_path = path.dirname(yaml_file_path)
    data_file_path = path.join(parent_dir_path,
      f"{path.basename(path.splitext(yaml_file_path)[0])}_metadata.{MetadataWrapper.FILENAME_EXTENSION}")
    return data_file_path

  @staticmethod
  def load(yaml_file_path):
    logging.info(f"loading metadata from {yaml_file_path}")
    instance = YamlSerializable.load(yaml_file_path)
    data = DataWrapper._load_data(instance.data_file_path)
    metadata = MetadataWrapper(instance.metadata_file_path)
    instance.set_data(data)
    instance.set_metadata(metadata)
    return instance

  @staticmethod
  def _load_data(data_file_path):
    try:
      logging.debug(f"loading data from {data_file_path}")
      data = xr.open_dataarray(data_file_path, decode_cf=False,
                               decode_times=False, decode_coords=False)
      return data
    except Exception as e:
      logging.error(f"cannot load data from '{data_file_path}': {str(e)}")
      raise e