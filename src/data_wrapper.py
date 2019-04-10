#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 10 11:38:36 2019

@author: sebastien@gardoll.fr
"""

from yaml_class import YamlSerializable
import logging
import h5py
import numpy as np
import os.path as path


class DataWrapper(YamlSerializable):

  HDF5_FILENAME_EXTENSION = 'h5'

  def __init__(self, str_id, data = None, data_file_path = None):
    super().__init__(str_id)

    self.data_file_path = data_file_path
    self._data = None

    if data is not None and data_file_path is not None:
      msg = 'parameter data and data_file_path are mutually exclusives'
      logging.error(msg)
      raise Exception(msg)
    else:
      if self.data_file_path is not None:
        data = DataWrapper._load_data(self.data_file_path)

      if data is not None:
        self.set_data(data)

  def set_data(self, data):
    self._data = data

  def get_data(self):
    return self._data

  def save(self, yaml_file_path):
    logging.info(f"saving tensor to {yaml_file_path}")

    if self._data is None:
      msg = "missing tensor data"
      logging.error(msg)
      raise Exception(msg)

    # Making self._data transient for yaml serialization.
    data = self._data
    del self._data

    if self.data_file_path is None:
      self.data_file_path = XarrayWrapper._compute_data_from_yaml_file_path(yaml_file_path)

    super().save(yaml_file_path)

    self._data = data
    self._save_data()

  def _save_data(self):
    try:
      logging.debug(f"saving tensor data to {self.data_file_path}")
      with h5py.File(self.data_file_path, 'w') as hdf5_file:
        hdf5_file.create_dataset('dataset', data=self._data)
    except Exception as e:
      logging.error(f"cannot save the tensor data to '{self.data_file_path}': {str(e)}")
      raise e

  @staticmethod
  def _compute_data_from_yaml_file_path(yaml_file_path):
    parent_dir_path = path.dirname(yaml_file_path)
    data_file_path = path.join(parent_dir_path,
      f"{path.basename(path.splitext(yaml_file_path)[0])}.{Tensor.HDF5_FILENAME_EXTENSION}")
    return data_file_path

  @staticmethod
  def load(yaml_file_path):
    logging.info(f"loading tensor from {yaml_file_path}")
    instance = YamlSerializable.load(yaml_file_path)
    data = XarrayWrapper._load_data(instance.data_file_path)
    instance.set_data(data)
    return instance

  @staticmethod
  def _load_data(data_file_path):
    try:
      logging.debug(f"loading tensor data from {data_file_path}")
      with h5py.File(data_file_path, 'r') as hdf5_file:
        data = hdf5_file.get('dataset')
        return np.array(data)
    except Exception as e:
      logging.error(f"cannot load tensor data from '{data_file_path}': {str(e)}")
      raise e