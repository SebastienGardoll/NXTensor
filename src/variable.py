#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 11:58:46 2019

@author: sebastien@gardoll.fr
"""

from yaml_class import YamlSerializable
import logging
from enum_utils import TimeResolution, CoordinateFormat, CoordinatePropertyKey,\
                       CoordinateKey
from abc import ABC, abstractmethod
import os.path as path

class Variable(YamlSerializable, ABC):

  FILE_NAME_POSTFIX = 'variable.yml'

  def __init__(self, str_id=None):
    super().__init__(str_id)

  def compute_filename(self):
    return Variable.generate_filename(self.str_id)

  @staticmethod
  def generate_filename(str_id):
    return f"{str_id}_{Variable.FILE_NAME_POSTFIX}"

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
    self.netcdf_attribute_name    = None
    self.netcdf_path_template     = None
    self.netcdf_period_resolution = None # Period covered by the netcdf file.

    self.time_resolution       = None  # Resolution of the time in the netcdf file.
    self.date_template         = None

    self.coordinate_metadata = dict()
    self.coordinate_metadata[CoordinateKey.LAT] =\
      {CoordinatePropertyKey.FORMAT    : CoordinateFormat.UNKNOWN,
       CoordinatePropertyKey.RESOLUTION: CoordinateFormat.UNKNOWN,
       CoordinatePropertyKey.NB_DECIMAL: CoordinateFormat.UNKNOWN,
       CoordinatePropertyKey.NETCDF_ATTR_NAME: None}

    self.coordinate_metadata[CoordinateKey.LON] = \
      {CoordinatePropertyKey.FORMAT    : CoordinateFormat.UNKNOWN,
       CoordinatePropertyKey.RESOLUTION: CoordinateFormat.UNKNOWN,
       CoordinatePropertyKey.NB_DECIMAL: CoordinateFormat.UNKNOWN,
       CoordinatePropertyKey.NETCDF_ATTR_NAME: None}

  def compute_netcdf_file_path(self, time_dict):
    return self.netcdf_path_template.format(**time_dict)

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
      logging.debug(f"loading the variables of {self.str_id}:")
      self._variables = list() # Preserve the order.
      for var_file_path in self.variable_file_paths:
        logging.debug(f"loading the variable {var_file_path}")
        var = Variable.load(var_file_path)
        self._variables.append(var)

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

  def __init__(self, time_dict):
    self.result = dict()
    self.time_dict = time_dict

  def visit_SingleLevelVariable(self, variable):
    current_dict = {variable.str_id: variable.compute_netcdf_file_path(self.time_dict)}
    self.result.update(current_dict)

  def visit_MultiLevelVariable(self, variable):
    self.visit_SingleLevelVariable(variable)

  def visit_ComputedVariable(self, variable):
    pass

def bootstrap_era5_variables(variable_parent_dir_path):
  era5_single_level_variables = ['msl', 'tcwv','u10', 'v10']
  time_resolution = TimeResolution.HOUR
  netcdf_period_resolution = TimeResolution.MONTH

  def bootstrap_era5_variable(str_id, attribute_name, netcdf_path_template, level = None):
    if level is None:
      variable = SingleLevelVariable()
    else:
      variable = MultiLevelVariable()
      variable.level = level

    variable.str_id = str_id
    variable.netcdf_attribute_name = attribute_name
    variable.time_resolution = time_resolution
    variable.netcdf_period_resolution = netcdf_period_resolution
    variable.date_template = '{year}-{month2d}-{day}T{hour}'
    variable.netcdf_path_template = netcdf_path_template

    cmdata = variable.coordinate_metadata

    lat_cmdata = cmdata[CoordinateKey.LAT]
    lat_cmdata[CoordinatePropertyKey.FORMAT] = CoordinateFormat.DECREASING_DEGREE_NORTH
    lat_cmdata[CoordinatePropertyKey.RESOLUTION] = 0.25
    lat_cmdata[CoordinatePropertyKey.NB_DECIMAL] = 2
    lat_cmdata[CoordinatePropertyKey.NETCDF_ATTR_NAME] = 'latitude'

    lon_cmdata = cmdata[CoordinateKey.LON]
    lon_cmdata[CoordinatePropertyKey.FORMAT] = CoordinateFormat.ZERO_TO_360_DEGREE_EAST
    lon_cmdata[CoordinatePropertyKey.RESOLUTION] = 0.25
    lon_cmdata[CoordinatePropertyKey.NB_DECIMAL] = 2
    lon_cmdata[CoordinatePropertyKey.NETCDF_ATTR_NAME] = 'longitude'

    variable_file_path = path.join(variable_parent_dir_path, variable.compute_filename())
    variable.save(variable_file_path)

  for str_id in era5_single_level_variables:
    netcdf_path_template = '/bdd/ERA5/NETCDF/GLOBAL_025/hourly/AN_SF/{year}/%s.{year}{month2d}.as1e5.GLOBAL_025.nc' % (str_id)
    bootstrap_era5_variable(str_id, str_id, netcdf_path_template)

  era5_multi_level_variables = [('ta200', 'ta', 200), ('ta500', 'ta', 500),
                                ('u850', 'u', 850), ('v850', 'v', 850)]
  time_resolution = TimeResolution.HOUR
  for str_id, attr_name, level in era5_multi_level_variables:
    netcdf_path_template = '/bdd/ERA5/NETCDF/GLOBAL_025/4xdaily/AN_PL/{year}/%s.{year}{month2d}.aphe5.GLOBAL_025.nc' % (attr_name)
    bootstrap_era5_variable(str_id, attr_name, netcdf_path_template, level)

"""
import logging
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

from variable import test_load
test_load('/home/sgardoll/cyclone/extraction_config')
"""
def test_load(variable_parent_dir_path):
  era5_variables = ['msl', 'tcwv','u10', 'v10', 'ta200', 'ta500', 'u850', 'v850']
  for str_id in era5_variables:
    var = Variable.load(path.join(variable_parent_dir_path, Variable.generate_filename(str_id)))
    print(var)

def create_computed_variables(variable_parent_dir_path):
  variable = ComputedVariable()
  variable.str_id = 'wsl'
  variable.computation_expression = 'u10 2 pow v10 2 pow + sqrt'
  variable.variable_file_paths = [path.join(variable_parent_dir_path,
                                            f"u10_{Variable.FILE_NAME_POSTFIX}"),
                                  path.join(variable_parent_dir_path,
                                            f"v10_{Variable.FILE_NAME_POSTFIX}")]
  variable.get_variables()
  variable_file_path = path.join(variable_parent_dir_path, variable.compute_filename())
  variable.save(variable_file_path)


"""
import logging
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

from variable import bootstrap_all
bootstrap_all('/home/sgardoll/cyclone/extraction_config')
"""
def bootstrap_all(config_file_parent_dir_path):
  bootstrap_era5_variables(config_file_parent_dir_path)
  create_computed_variables(config_file_parent_dir_path)