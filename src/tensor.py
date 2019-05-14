#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 27 11:58:30 2019

@author: sebastien@gardoll.fr
"""

from data_wrapper import DataWrapper
import logging
from enum_utils import CoordinateKey, CoordinateFormat, TensorKey

class Tensor(DataWrapper):

  def __init__(self, str_id, data, metadata, coordinate_format, is_channels_last,
               channel_id_to_index):

    super().__init__(str_id, data, metadata)
    self.is_channels_last = is_channels_last
    self.coordinate_format = coordinate_format

    # Dictionary that maps channel'str_id to their index in the tensor.
    self.channel_id_to_index = channel_id_to_index

    # Array that maps index of channel in data to their str_id.
    if channel_id_to_index is not None:
      self.index_to_channel = dict(zip(channel_id_to_index.values(), channel_id_to_index.keys()))
    else:
      self.index_to_channel = None

  # None Proof
  def __init_data_properties(self, shape):
    if shape is not None:
      if len(shape) == 4:
        if self.is_channels_last:
          x_size     = shape[1]
          y_size     = shape[2]
          nb_channel = shape[3]
        else:
          x_size     = shape[2]
          y_size     = shape[3]
          nb_channel = shape[1]
      else:
        if len(shape) == 3:
          x_size = shape[1]
          y_size = shape[2]
          nb_channel = 1
        else:
          msg = 'unsupported shape of data'
          logging.error(msg)
          raise Exception(msg)

      nb_img = shape[0]
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

  def __len__(self):
    return self.nb_img

  # None Proof.
  def set_data(self, data):
    super().set_data(data)
    if data is not None:
      self.__init_data_properties(data.shape)
    else:
      self.__init_data_properties(None)

  def append(self, other):
    nb_self_dim  = len(self.shape)
    nb_other_dim = len(other.shape)

    if nb_self_dim == nb_other_dim:
      if nb_self_dim == 3:
        dim_name   = TensorKey.CHANNEL
      else:
        if nb_self_dim == 4:
          dim_name = TensorKey.IMG
        else:
          msg = f"number of dimension unsupported: {nb_self_dim}"
          logging.error(msg)
          raise Exception(msg)
    else:
      if nb_self_dim == (nb_other_dim + 1):
        dim_name = self.get_data().dims[0]
      else:
        msg = f"unsupported case of appending, nb_self_dim: {nb_self_dim}, nb_other_dim: {nb_other_dim}"
        logging.error(msg)
        raise Exception(msg)

    super().append(other, dim_name)

  # Return a list of instance of Tensor according to the given numpy'style split
  # specifications.
  def split(self, split_spec):
    msg = 'Tensor.split is not implemented yet'
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

  def _inner_standardize(data_array, mean=None, std=None):
    if mean is None:
      mean = data_array.mean()

    if std is None:
      std  = data_array.std()

    data_array -= mean
    data_array /= std
    return mean, std

  def _dispatcher(self, channel_method, img_tensor_method, **kwargs):
    if self.shape == 3:
      channel_method(kwargs)
    else:
      if self.shape == 4:
        img_tensor_method(kwargs)
      else:
        msg = f"unsupported shape ({self.shape}) of tensor"
        logging.fatal(msg)
        raise Exception(msg)

  def __repr__(self):
    return f"{self.__class__.__name__}(str_id={self.str_id}, shape={self.shape})"

"""
import logging
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)-8s %(message)s')
handler.setFormatter(formatter)
if logger.hasHandlers():
  logger.handlers.clear()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

from tensor import unit_test
"""
def unit_test():
  # Don't do that !
  data = Tensor._load_data('/data/sgardoll/cyclone_data/tensor/test_2kb_tensor.h5')
  str_id = 'test_2kb'
  is_channels_last = True
  channel_to_index = None
  metadata = None
  coordinate_format = {CoordinateKey.LAT: CoordinateFormat.UNKNOWN,
                       CoordinateKey.LON: CoordinateFormat.UNKNOWN}
  tensor = Tensor(str_id, data, metadata, coordinate_format, is_channels_last,
                  channel_to_index)
  print(tensor)
  print(tensor.shape)
  print(tensor.x_size)
  print(tensor.y_size)
  print(tensor.nb_channel)
  print(tensor.nb_img)
  print(tensor.get_data().shape)

  yaml_file_path = '/home/sgardoll/tmp/test.yml'
  tensor.save(yaml_file_path)

  tensor = Tensor.load(yaml_file_path)
  print(tensor)