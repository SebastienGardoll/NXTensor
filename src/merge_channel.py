#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 16:46:23 2019

@author: sebastien@gardoll.fr
"""

from config_extraction import ExtractionConfig
from tensor import Tensor
from os import path

import logging

class MergeChannel:

  def __init__(self, extraction_config_path):
    self.extraction_conf = ExtractionConfig.load(extraction_config_path)
    self.channel_paths = list()
    for variable in self.extraction_conf.get_variables():
      channel_filename = Tensor.compute_tensor_name(self.extraction_conf.str_id, variable.str_id)
      channel_path = path.join(self.extraction_conf.tensor_dir_path, channel_filename)
      self.channel_paths.append(channel_path)

  def merge_channels(self):
    tensors = list()

    for channel_path in self.channel_paths:
      logging.info(f"loading channel at {channel_path}")
      tensor = Tensor.load(channel_path)
      tensors.append(tensor)

    logging.info(f"the {len(self.channel_paths)} channels are loaded")
    result = tensors.pop(0)
    logging.info(f"stacking the {len(self.channel_paths)} channels")
    result.stack(tensors)
    logging.info(f"the tensor shape is {result.shape}")
    return result

"""
import logging
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - : %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
handler.setFormatter(formatter)
if logger.hasHandlers():
  logger.handlers.clear()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

from merge_channel import unit_test
unit_test('/home/sgardoll/cyclone/extraction_config')
"""
def unit_test(config_parent_path):
  from os import path
  extraction_config_path = path.join(config_parent_path, '2000_10_extraction_config.yml')
  driver = MergeChannel(extraction_config_path)
  stacked_tensor = driver.merge_channels()
