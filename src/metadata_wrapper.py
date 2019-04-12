#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 12 10:23:54 2019

@author: sebastien@gardoll.fr
"""

import pandas as pd
import numpy as np
import logging

class MetadataWrapper:

  FILENAME_EXTENSION    = 'csv'

  CSV_SEPARATOR          = ','
  CSV_NA_SYMBOLE         = ''
  CSV_HEADER_LINE_NUMBER = 0
  CSV_LINE_TERMINATOR    = '\n'
  CSV_ENCODING           = 'utf8'

  def __init__(self, dataframe = None, csv_file_path = None):
    self.csv_file_path = None
    self._dataframe     = None

    if dataframe is not None:
      self.set_dataframe(dataframe)
    else:
      if csv_file_path is not None:
        dataframe = self._load(csv_file_path)
        self.csv_file_path = csv_file_path
        self.set_dataframe(dataframe)
      else:
        msg = 'one other parameter is necessary (dataframe or csv_file_path)'
        logging.error(msg)
        raise Exception(msg)

  def append(self, other):
    new_dataframe = self.get_dataframe().append(other=other.get_dataframe(),
                                                ignore_index = True)
    self.set_dataframe(new_dataframe)

  def __len__(self):
    return len(self.get_dataframe().index)

  def shuffle(self, permutations = None):
    dataframe = self.get_dataframe()
    if permutations is None:
      permutations = np.random.permutation(len(self))

    # Even if copy is False, dataframe variable has to be re-affected.
    dataframe = dataframe.reindex(labels = permutations, copy = False)
    dataframe.reset_index(drop=True, inplace = True)

    self.set_dataframe(dataframe)

    return permutations

  def close(self):
    pass # Nothing to do.

  def set_dataframe(self, dataframe):
    self._dataframe = dataframe

  def get_dataframe(self):
    return self._dataframe

  def save(self, csv_file_path):
    logging.info(f"saving dataframe to '{self.csv_file_path}'")
    self.get_dataframe().to_csv(path_or_buf=csv_file_path,
                     sep = MetadataWrapper.CSV_SEPARATOR,
                     na_rep = MetadataWrapper.CSV_NA_SYMBOLE, header = True,
                     index = True, index_label='index',
                     encoding = MetadataWrapper.CSV_ENCODING,
                     line_terminator = MetadataWrapper.CSV_LINE_TERMINATOR)

  def _load(self):
    logging.info(f"opening dataframe '{self.csv_file_path}'")
    with open(self.csv_file_path, 'r') as csv_file:
      try:
        dataframe = pd.read_csv(filepath_or_buffer=csv_file,
                               sep=MetadataWrapper.CSV_SEPARATOR,
                               header=MetadataWrapper.CSV_HEADER_LINE_NUMBER,
                               na_values=MetadataWrapper.CSV_NA_SYMBOLE,
                               lineterminator=MetadataWrapper.CSV_LINE_TERMINATOR,
                               encoding=MetadataWrapper.CSV_ENCODING)
        self.set_dataframe(dataframe)
      except:
        msg = f"error while loading dataframe from {self.csv_file_path}"
        logging.error(msg)
        raise Exception(msg)
