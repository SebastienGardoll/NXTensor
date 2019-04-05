#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  3 17:27:08 2019

@author: sebastien@gardoll.fr
"""

from extraction import ExtractionConfig
from db_handler import DbHandler

class ChannelExtraction:

  def __init__(self, extraction_config_path, variable_index):
    self._extraction_conf = ExtractionConfig.load(extraction_config_path)
    self.variable_index = variable_index
    self.extracted_variable = self._extraction_conf.get_variables()[variable_index]
    self._label_dbs = list()

    for label in self._extraction_conf.get_labels():
      current_db = DbHandler.load(label.db_file_path,
                                  label.db_format,
                                  label.db_meta_data_mapping,
                                  label.db_format_options)
      self._label_dbs.append(current_db)

  def extract(self):
    pass

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
  variable_index = 0
  driver = ChannelExtraction(extraction_config_path, variable_index)