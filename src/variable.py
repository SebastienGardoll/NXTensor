#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 11:58:46 2019

@author: sebastien@gardoll.fr
"""

from yaml_class import YamlSerializable
import logging
import time_utils as tu
from enum_utils import TimeResolution, CoordinateFormat
from abc import ABC, abstractmethod
import os.path as path

class Variable(YamlSerializable, ABC):

  FILE_NAME_POSTFIX = '_variable.yml'

  def __init__(self, str_id=None):
    super().__init__(str_id)

  def __repr__(self):
    return f"{self.__class__.__name__}(str_id={self.str_id})"

  def compute_yaml_filename(self):
    return f"{self.str_id}{Variable.FILE_NAME_POSTFIX}"

  @abstractmethod
  def accept(self, visitor):
    pass

  @staticmethod
  def infer_from_netcdf(file_path):
    msg = 'infer_from_netcdf is not implemented yet'
    logging.error(msg)
    raise NotImplementedError(msg)

class SingleLevelVariable(Variable):

  def __init__(self, str_id=None):
    super().__init__(str_id)
    self.netcdf_attribute_name = None
    self.netcdf_path_template  = None

    self.time_resolution       = None
    self.date_template         = None

    self.lat_attribute_name    = None
    self.lat_format            = None
    self.lat_resolution        = None
    self.nb_lat_decimal        = None

    self.lon_attribute_name    = None
    self.lon_format            = None
    self.lon_resolution        = None
    self.nb_lon_decimal        = None

  # Date is expected to be a datetime instance.
  def compute_netcdf_file_path(self, date):
    kwargs = tu.build_date_dictionary(date)
    return self.netcdf_path_template.format(**kwargs)

  def accept(self, visitor):
    visitor.visit_SingleLevelVariable(self)

class MultiLevelVariable(SingleLevelVariable):

  def __init__(self, str_id=None):
    super().__init__(str_id)
    self.level    = None

  def accept(self, visitor):
    visitor.visit_MultiLevelVariable(self)

class ComputedVariable(Variable):

  def __init__(self, str_id=None):
    super().__init__(str_id)
    self.variable_file_paths    = None
    self.computation_expression = None # Using Reverse Polish Notation !
    self._variables             = None # Transient for yaml serialization.

  def get_variables(self):
    variables_value = getattr(self, '_variables', None)
    if variables_value is None:
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


def bootstrap_era5_variable(variable_parent_dir_path, str_id, attribute_name,
                            time_resolution, netcdf_path_template, level = None):
  if level is None:
    variable = SingleLevelVariable()
  else:
    variable = MultiLevelVariable()
    variable.level = level

  variable.str_id = str_id
  variable.netcdf_attribute_name = attribute_name
  variable.time_resolution = time_resolution
  variable.date_template = '{year}-{month}-{day}T{hour}:{minute}:{second}.{microsecond}'
  variable.lat_attribute_name = 'latitude'
  variable.lon_attribute_name = 'longitude'
  variable.lat_format = CoordinateFormat.DECREASING_DEGREE_NORTH
  variable.lon_format = CoordinateFormat.AMERICAN_DEGREE_EAST
  variable.netcdf_path_template = netcdf_path_template
  variable.lat_resolution = 0.25
  variable.lon_resolution = 0.25
  variable.nb_lat_decimal = 2
  variable.nb_lon_decimal = 2

  variable_file_path = path.join(variable_parent_dir_path, variable.compute_yaml_filename())
  variable.save(variable_file_path)

"""
import logging
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

from variable import bootstrap_era5_variables
bootstrap_era5_variables('...')
"""
def bootstrap_era5_variables(variable_parent_dir_path):
  era5_single_level_variables = ['msl', 'tcwv','u10', 'v10']
  time_resolution = TimeResolution.HOUR
  for str_id in era5_single_level_variables:
    netcdf_path_template = '/bdd/ERA5/NETCDF/GLOBAL_025/hourly/AN_SF/{year}/%s.{year}{month2d}.as1e5.GLOBAL_025.nc' % (str_id)
    bootstrap_era5_variable(variable_parent_dir_path, str_id, str_id,
                            time_resolution, netcdf_path_template)

  era5_multi_level_variables = [('ta200', 'ta', 200), ('ta500', 'ta', 500),
                                ('u850', 'u', 850), ('v850', 'v', 850)]
  time_resolution = TimeResolution.SIX_HOURS
  for str_id, attr_name, level in era5_multi_level_variables:
    netcdf_path_template = '/bdd/ERA5/NETCDF/GLOBAL_025/4xdaily/AN_PL/{year}/%s.{year}{month2d}.aphe5.GLOBAL_025.nc' % (attr_name)
    bootstrap_era5_variable(variable_parent_dir_path, str_id, attr_name,
                            time_resolution, netcdf_path_template, level)

"""
import logging
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

from variable import test_load
test_load('...')
"""
def test_load(variable_parent_dir_path):
  era5_variables = ['msl', 'tcwv','u10', 'v10', 'ta200', 'ta500', 'u850', 'v850']
  for str_id in era5_variables:
    var = Variable.load(path.join(variable_parent_dir_path, f"{str_id}{Variable.FILE_NAME_POSTFIX}"))
    print(var)

"""
import logging
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

from variable import create_computed_variables
create_computed_variables('...')
"""
def create_computed_variables(variable_parent_dir_path):
  variable = ComputedVariable()
  variable.str_id = 'wsl'
  variable.computation_expression = 'u10 v10 +'
  variable.variable_file_paths = [path.join(variable_parent_dir_path,
                                            f"u10{Variable.FILE_NAME_POSTFIX}"),
                                  path.join(variable_parent_dir_path,
                                            f"v10{Variable.FILE_NAME_POSTFIX}")]
  variable.get_variables()
  variable_file_path = path.join(variable_parent_dir_path, variable.compute_yaml_filename())
  variable.save(variable_file_path)