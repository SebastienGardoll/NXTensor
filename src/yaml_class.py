#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 27 13:35:52 2019

@author: sebastien@gardoll.fr
"""

import yaml
import logging

class YamlSerializable:

  YAML_FILENAME_EXT = 'yml'

  def __init__(self):

    # Public class members:
    self.str_id = None

  def __init__(self, str_id):
    self.str_id = str_id

    # Save this instance to the given path (override if it already exists)
  def save(self, file_path):
    try:
      logging.info(f"saving {self.__class__.__name__} '{self.str_id}' to '{file_path}'")
      yml_content = yaml.dump(self, default_flow_style=False, indent=2)
    except Exception as e:
      logging.error(f"cannot serialize {self.__class__.__name__}: {str(e)}")
      raise e

    try:
      with open(file_path, 'w') as file:
        file.write(yml_content)
    except Exception as e:
      logging.error(f"cannot save {self.__class__.__name__} to '{file_path}': {str(e)}")
      raise e

  @staticmethod
  def load(file_path):
    try:
      logging.info(f"read file from '{file_path}'")
      with open(file_path, 'r') as file:
        yml_content = file.read()
    except Exception as e:
      logging.error(f"cannot open file '{file_path}': {str(e)}")
      raise e

    try:
      return yaml.load(yml_content)
    except Exception as e:
      logging.error(f"cannot deserialize: {str(e)}")
      raise e