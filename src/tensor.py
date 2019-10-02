#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 27 11:58:30 2019

@author: sebastien@gardoll.fr
"""

from data_wrapper import DataWrapper
import logging
from enum_utils import CoordinateKey, CoordinateFormat, TensorKey
import numpy as np
from xarray import DataArray

class Tensor(DataWrapper):

  def __init__(self, str_id, data, metadata, coordinate_format, is_channels_last):
    super().__init__(str_id, data, metadata)
    self.is_channels_last = is_channels_last
    self.coordinate_format = coordinate_format
    self.stats_mapping = dict()

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

  def compute_tensor_name(extraction_id, variable_id):
    return f"{extraction_id}_{variable_id}_{TensorKey.CHANNEL}.{Tensor.YAML_FILENAME_EXT}"

  def append(self, other):
    nb_self_dim  = len(self.shape)
    nb_other_dim = len(other.shape)
    logging.info(f"append data (shape: {other.shape}) to this data (shape: {self.shape})")

    if nb_self_dim == nb_other_dim:
      if nb_self_dim == 3:
        dim_name = self._data.dims[0]
      else:
        if nb_self_dim == 4:
          dim_name = TensorKey.IMG # TODO: to be tested.
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
    logging.info(f"new shape is {self.shape}")

  # Return a list of instance of Tensor according to the given split
  # specifications: (train_ratio, [validation_ratio], [test_ratio]).
  def split(self, split_spec):

    # TODO: check addition <= 1. ; each element <0 < 1.

    msg = 'Tensor.split is not implemented yet'
    logging.error(msg)
    raise NotImplementedError(msg)

  def stack(self, other_tensors, str_id):

    if not self.is_channel():
      msg = 'Tensor.stack is not implemented for data with number of dimension over 3'
      logging.fatal(msg)
      raise NotImplementedError(msg)

    data_arrays = [self.get_data()]
    for other_tensor in other_tensors:
      if not other_tensor.is_channel():
        msg = 'Tensor.stack is not implemented for data with number of dimension over 3'
        logging.fatal(msg)
        raise NotImplementedError(msg)
      data_arrays.append(other_tensor.get_data())

    numpy_data = np.stack(data_arrays, axis=3)
    dims = (TensorKey.IMG, TensorKey.X, TensorKey.Y, TensorKey.CHANNEL)
    data = DataArray(numpy_data, dims=dims)
    self.set_data(data) # Close the previous data array object.
    self.is_channels_last = True
    self.str_id = str_id

    for other_tensor in other_tensors:
      self.stats_mapping.update(other_tensor.stats_mapping)

  def is_channel(self):
    return len(self.shape) == 3

  def standardize(self, channel_name=None, mean=None, std=None):
    if not self.is_channel():
      msg = 'Tensor.standardize is not implemented yet for tensor (but it is for channel)'
      logging.error(msg)
      raise NotImplementedError(msg)

    logging.debug("standardizating the channel")

    if channel_name is None:
      channel_name = self._data.dims[0]

    # if stats_mapping not None => standardize the tensor according to the given mapping
    # else compute std, apply and save it.
    mean, std = self._inner_standardize(mean, std)

    if not channel_name in self.stats_mapping:
      mapping = {'mean': 0., 'std': 0.}
      self.stats_mapping[channel_name]= mapping
    else:
      mapping = self.stats_mapping[channel_name]

    mapping[TensorKey.MEAN] = mean
    mapping[TensorKey.STD]  = std

  def _inner_standardize(self, mean=None, std=None):
    data_array = self.get_data()

    if mean is None:
      mean = float(data_array.mean())

    if std is None:
      std  = float(data_array.std())

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
  with Tensor(str_id, data, metadata, coordinate_format, is_channels_last,
              channel_to_index) as tensor:
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