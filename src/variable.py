#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 11:58:46 2019

@author: sebastien@gardoll.fr
"""

from yaml_class import YamlSerializable
import logging
import time_utils as tu
from abc import ABC, abstractmethod

class Variable(YamlSerializable, ABC):

  def __init__(self):
    super().__init__()

  def __init__(self, str_id):
    super().__init__(str_id)

  def __repr__(self):
    return f"{self.__class__.__name__}(str_id={self.str_id})"

  @abstractmethod
  def accept(self, visitor):
    pass

  @staticmethod
  def infer_from_netcdf(file_path):
    msg = 'infer_from_netcdf is not implemented yet'
    logging.error(msg)
    raise NotImplementedError(msg)

class SingleLevelVariable(Variable):

  def __init__(self):
    super().__init__()
    self.root_dir_path         = None
    self.netcdf_attribute_name = None
    self.time_resolution       = None
    self.date_template         = None
    self.lat_attribute_name    = None
    self.lon_attribute_name    = None
    self.coord_format          = None
    self.netcdf_path_template  = None
    self.lat_resolution        = None
    self.lon_resolution        = None
    self.nb_lat_decimal        = None
    self.nb_lon_decimal        = None

  # Date is expected to be a datetime instance.
  def compute_netcdf_file_path(self, date):
    kwargs = tu.build_date_dictionary(date)
    return self.netcdf_path_template.format(**kwargs)

  def accept(self, visitor):
    visitor.visit_SingleLevelVariable(self)


class MultiLevelVariable(SingleLevelVariable):

  def __init__(self):
    super().__init__()
    self.level    = None

  def accept(self, visitor):
    visitor.visit_MultiLevelVariable(self)

class ComputedVariable(Variable):

  def __init__(self):
    super().__init__()
    self.variable_file_paths    = None
    self.computation_expression = None # Using Reverse Polish Notation !
    self._variables             = None # Transient for yaml serialization.

  def get_variables(self):
    if self._variables is None:
      logging.debug('loading the variables of {self.str_id}:')
      self._variables = dict()
      for var_file_path in self.variable_file_paths:
        logging.debug('  loading the variable {var_file_path}')
        var = Variable.load(var_file_path)
        self._variables[var.str_id] = var

    return self._variables

  def save(self, file_path):
    variables = self._variables
    del self._variables
    super().save(file_path)
    self._variables = variables

  def accept(self, visitor):
    for variable in self.get_variables():
      variable.accept(visitor)

    visitor.visit_ComputedVariable(self)

class VariableVisitor(ABC):

  @abstractmethod
  def visit_SingleLevelVariable(self, variable):
    pass

  @abstractmethod
  def visit_MultiLevelVariable(self, variable):
    pass

  @abstractmethod
  def visit_ComputedVariable(self, variable):
    pass


class VariableNetcdfFilePathVisitor(VariableVisitor):

  def __init__(self, date):
    self.result = dict()
    self.date = date

  def visit_SingleLevelVariable(self, variable):
    current_dict = {variable.str_id: variable.compute_netcdf_file_path(self.date)}
    self.result.update(current_dict)

  def visit_MultiLevelVariable(self, variable):
    self.visit_SingleLevelVariable(variable)

  def visit_ComputedVariable(self, variable):
    pass