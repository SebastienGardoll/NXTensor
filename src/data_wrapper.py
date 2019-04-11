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
from abc import abstractmethod
import numpy as np
from enum_utils import TensorKey

class DataWrapper(YamlSerializable):

  FILENAME_EXTENSION = 'nc'

  def __init__(self, str_id, data = None, data_file_path = None, shape = None):
    super().__init__(str_id)

    self.data_file_path = None
    self.shape = None
    self._data = None

    if data is not None:
      self.set_data(data)
    else:
      if data_file_path is not None:
        data = DataWrapper._load_data(self.data_file_path)
        self.data_file_path = data_file_path
        self.set_data(data)
      else:
        if shape is not None:
          data = xr.DataArray(np.ndarray(shape, dtype=float))
          self.shape = shape
          self.set_data(data)
        else:
          msg = 'one other parameter is necessary (data, data_file_path or shape)'
          logging.error(msg)
          raise Exception(msg)

  def append(self, other):
    nb_self_dim  = len(self.shape)
    nb_other_dim = len(other.shape)

    if nb_self_dim == nb_other_dim:
      if nb_self_dim == 2:
        dim = TensorKey.CHANNEL
      else:
        dim = TensorKey.IMG

      new_data = xr.concat((self.get_data(), other.get_data()),
                              dim=dim)
    else:
      if nb_self_dim == (nb_other_dim + 1):
        new_data = xr.concat((self.get_data(), other.get_data()),
                              dim=self.get_data().dims[0])
      else:
        msg = f"unsupported case of appending, nb_self_dim: {nb_self_dim}, nb_other_dim: {nb_other_dim}"
        logging.error(msg)
        raise Exception(msg)

    new_instance = self._copy_metadata(new_data)
    return new_instance

  @abstractmethod
  def copy_metadata(self, data = None):
    pass

  def set_data(self, data):
    self._data = data
    self.shape = data.shape

  def get_data(self):
    return self._data

  def close(self):
    try:
      self._data.close()
    except :
      pass

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    self.close()

  def save(self, yaml_file_path):
    logging.info(f"saving metadata to {yaml_file_path}")

    if self._data is None:
      msg = "missing data"
      logging.error(msg)
      raise Exception(msg)

    # Making self._data transient for yaml serialization.
    data = self._data
    del self._data

    if self.data_file_path is None:
      self.data_file_path = DataWrapper._compute_data_from_yaml_file_path(yaml_file_path)

    super().save(yaml_file_path)

    self._data = data
    self._save_data(self.data_file_path)

  def _save_data(self, data_file_path):
    try:
      logging.debug(f"saving data to {self.data_file_path}")
      self.data.to_netcdf(data_file_path, mode = 'w')
    except Exception as e:
      logging.error(f"cannot save the data to '{self.data_file_path}': {str(e)}")
      raise e

  @staticmethod
  def _compute_data_from_yaml_file_path(yaml_file_path):
    parent_dir_path = path.dirname(yaml_file_path)
    data_file_path = path.join(parent_dir_path,
      f"{path.basename(path.splitext(yaml_file_path)[0])}.{DataWrapper.FILENAME_EXTENSION}")
    return data_file_path

  @staticmethod
  def load(yaml_file_path):
    logging.info(f"loading metadata from {yaml_file_path}")
    instance = YamlSerializable.load(yaml_file_path)
    data = DataWrapper._load_data(instance.data_file_path)
    instance.set_data(data)
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