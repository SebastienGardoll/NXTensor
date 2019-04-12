#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 27 11:58:30 2019

@author: sebastien@gardoll.fr
"""

from data_wrapper import DataWrapper
import logging

class Tensor(DataWrapper):

  def __init__(self, str_id, data, metadata, is_channels_last, channel_to_index):

    super().__init__(str_id, data, metadata)
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

  # None Proof.
  def set_data(self, data):
    super().set_data(data)
    if data is not None:
      self.__init_data_properties(data.shape)
    else:
      self.__init_data_properties(None)

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

  def __repr__(self):
    return f"{self.__class__.__name__}(str_id={self.str_id}, shape={self.shape})"

"""
import logging
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)-8s %(message)s')
handler.setFormatter(formatter)
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
  index_to_localisation = None
  tensor = Tensor(str_id, is_channels_last, channel_to_index, index_to_localisation,
                  data)
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