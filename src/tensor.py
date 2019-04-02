#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 27 11:58:30 2019

@author: sebastien@gardoll.fr
"""

from yaml_class import YamlSerializable
import logging
import h5py
import numpy as np
import os.path as path


########## IMPLEMENT WITH XARRAY ONLY IF ALL THE METHODS ARE ABLE TO BE ########

class Tensor(YamlSerializable):

  HDF5_FILENAME_EXTENSION = 'h5'

  def __init__(self, str_id, is_channels_last, channel_to_index,
               index_to_localisation, lazy_loading = False, data = None,
               data_file_path = None):

    super().__init__(str_id)
    self.is_channels_last = is_channels_last

    # Dictionary that maps channel'str_id to their index in the tensor.
    self.channel_to_index = channel_to_index

    # Array that maps index of channel in data to their str_id.
    if channel_to_index is not None:
      self.index_to_channel = list()
      for k, v in channel_to_index.items():
        self.index_to_channel[v] = k
    else:
      self.index_to_channel = None

    # Array that map index of data row to their geo-localisation.
    self.index_to_localisation = index_to_localisation

    self.data_file_path = data_file_path # Should be set after calling save or load.

    if data is not None and data_file_path is not None:
      msg = 'parameter data and data_file_path are mutually exclusives'
      logging.error(msg)
      raise Exception(msg)
    else:
      self.set_data(data) # None proof.
      if not lazy_loading and data_file_path is not None:
        self.get_data()

  # None Proof
  def __init_data_properties(self, shape):
    if shape is not None:
      if self.is_channels_last:
        x_size     = shape[1]
        y_size     = shape[2]
        nb_channel = shape[3]
      else:
        x_size     = shape[2]
        y_size     = shape[3]
        nb_channel = shape[1]

      nb_img       = shape[0]
    else:
      x_size     = None
      y_size     = None
      nb_channel = None
      nb_img     = None

    self.x_size     = x_size # In pixels.
    self.y_size     = y_size # In pixels.
    self.shape      = shape # Tuple from numpy.
    self.nb_channel = nb_channel
    self.nb_img     = nb_img

  # None Proof.
  def set_data(self, data):
    self._data = data
    if data is not None:
      self.__init_data_properties(data.shape)
    else:
      self.__init_data_properties(None)

  def save(self, yaml_file_path):
    logging.info(f"saving tensor to {yaml_file_path}")

    if self._data is None:
      msg = "missing tensor data"
      logging.error(msg)
      raise Exception(msg)

    # Making self._data transient for yaml serialization.
    data = self._data()
    del self._data

    if self.data_file_path is None:
      self.data_file_path = Tensor._compute_data_from_yaml_file_path(yaml_file_path)

    super().save(yaml_file_path)

    self._data = data
    self._save_data(self.data_file_path)

  def _save_data(self):
    try:
      logging.debug(f"saving tensor data to {self.data_file_path}")
      hdf5_file = h5py.File(self.data_file_path, 'w')
      hdf5_file.create_dataset('dataset', data=self._data)
      hdf5_file.close()
    except Exception as e:
      logging.error(f"cannot save the tensor data to '{self.data_file_path}': {str(e)}")
      raise e

  def get_data(self):
    if self._data is None:
      if self.data_file_path is not None: # Lazy loading.
        data = Tensor._load_data(self.data_file_path)
        self.set_data(data)
      else:
        msg = "missing data file path"
        logging.error(msg)
        raise Exception(msg)
    return self._data

  # Return a new instance of Tensor with its data shuffled (keep geolocalisation
  # consistent)
  def shuffle(self):
    msg = 'Tensor.shuffle is not implemented yet'
    logging.error(msg)
    raise NotImplementedError(msg)

  # Return a list of instance of Tensor according to the given numpy'style split
  # specifications.
  def split(self, split_spec):
    msg = 'Tensor.split is not implemented yet'
    logging.error(msg)
    raise NotImplementedError(msg)

  def concat(self, other_tensors):
    msg = 'Tensor.concat is not implemented yet'
    logging.error(msg)
    raise NotImplementedError(msg)

  def stack(self, other_tensors):
    msg = 'Tensor.stack is not implemented yet'
    logging.error(msg)
    raise NotImplementedError(msg)

  def standardize(self, stats_mapping):
    # Dictionary that maps channel str_id and the path of the statistics file
    # of the channel.

    # if stats_mapping not None => standardize the tensor according to the given mapping
    # else compute std, apply and save it.
    self.stats_mapping = stats_mapping

    msg = 'Tensor.standardize is not implemented yet'
    logging.error(msg)
    raise NotImplementedError(msg)

  @staticmethod
  def _compute_data_from_yaml_file_path(yaml_file_path):
    parent_dir_path = path.dirname(yaml_file_path)
    data_file_path = path.join(parent_dir_path,
      f"{path.basename(path.splitext(yaml_file_path)[0])}.{Tensor.HDF5_FILENAME_EXTENSION}")
    return data_file_path

  @staticmethod
  def load(yaml_file_path):
    logging.info(f"loading tensor from {yaml_file_path}")
    tensor_instance = YamlSerializable.load(yaml_file_path)
    data = Tensor._load_data(tensor_instance.data_file_path)
    tensor_instance.set_data(data)
    return tensor_instance

  @staticmethod
  def _load_data(data_file_path):
    try:
      logging.debug(f"loading tensor data from {data_file_path}")
      hdf5_file = h5py.File(data_file_path, 'r')
      data = hdf5_file.get('dataset')
      return np.array(data)
    except Exception as e:
      logging.error(f"cannot load tensor data from '{data_file_path}': {str(e)}")
      raise e