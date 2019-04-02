#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 27 11:57:37 2019

@author: sebastien@gardoll.fr
"""

import dask
import xarray as xr
import logging
from variable import MultiLevelVariable, SingleLevelVariable,\
ComputedVariable, VariableNetcdfFilePathVisitor, VariableVisitor, Variable
import coordinate_utils as cu
import time_utils as tu
from xarray_utils import XarrayRpnCalculator, DEFAULT_DASK_SCHEDULER

class XarrayTimeSeries:

  # Date is expected to be a datetime instance.
  def __init__(self, variable, date):#, dask_scheduler, nb_workers):
    logging.debug(f"visiting the file paths of the variable '{variable.str_id}'")
    visitor = VariableNetcdfFilePathVisitor(date)
    variable.accept(visitor)
    netcdf_file_path_dict = visitor.result
    self.variable = variable
    self.date = date
    self.dask_scheduler = DEFAULT_DASK_SCHEDULER
    self._open_netcdf(list(netcdf_file_path_dict.values()))

  # Options is a dictionary of named parameters for the methods open_mfdataset or
  # open_dataset.
  def _open_netcdf(self, netcdf_file_paths, options = None):
    logging.info(f"opening netcdf files: {netcdf_file_paths}")
    try:
      if len(netcdf_file_paths) > 1:
        if options is not None:
          self.dataset = xr.open_mfdataset(netcdf_file_paths, **options)
        else:
          self.dataset = xr.open_mfdataset(netcdf_file_paths)
      else:
        if options is not None:
          self.dataset = xr.open_dataset(netcdf_file_paths[0], **options)
        else:
          self.dataset = xr.open_dataset(netcdf_file_paths[0])
    except Exception as e:
      msg = f"unable to open netcdf file from '{netcdf_file_paths}': {str(e)}"
      logging.error(msg)
      raise e

  def __repr__(self):
    return f"{self.__class__.__name__}(variable={self.variable}, date={self.date})"

  # Extract the region that centers the given lat/lon location.
  def extract_square_region(self, variable, date, lat, lon,
                            half_lat_frame, half_lon_frame):
    logging.info(f"extracting subregion {date}, {lat}, {lon} for variable '{variable.str_id}'")
    if isinstance(variable, MultiLevelVariable) or \
       isinstance(variable, SingleLevelVariable):
      logging.debug(f"starting the extraction of the variable '{variable.str_id}'")
      result = self._extract_square_region(variable, date, lat, lon,
                                           half_lat_frame, half_lon_frame)
    else:
      if isinstance(variable, ComputedVariable):
        logging.debug(f"starting the extraction of the computed variable '{variable.str_id}'")
        visitor = ExtractionComputedVariable(self, date, lat, lon,
                                             half_lat_frame, half_lon_frame)
        variable.accept(visitor)
        result = visitor.result
      else:
        msg = f"unsupported variable type '{type(variable)}'"
        logging.error(msg)
        raise Exception(msg)
    return result

  # Extract the region that centers the given lat/lon location. Variable must be
  # SingleLevelVariable or MultiLevelVariable.
  def _extract_square_region(self, variable, date, lat, lon,
                             half_lat_frame, half_lon_frame):
    logging.debug(f"extracting subregion {date}, {lat}, {lon} for variable '{variable.str_id}'")
    rounded_lat = cu.round_nearest(lat, variable.lat_resolution, variable.nb_lat_decimal)
    rounded_lon = cu.round_nearest(lon, variable.lon_resolution, variable.nb_lon_decimal)

    # Minus LAT_RESOLUTION because the upper bound in slice is included.
    lat_min  = (rounded_lat - half_lat_frame + variable.lat_resolution)
    lat_max  = (rounded_lat + half_lat_frame)
    # Minus LON_RESOLUTION because the upper bound in slice is included.
    lon_min  = (rounded_lon - half_lon_frame)
    lon_max  = (rounded_lon + half_lon_frame - variable.lon_resolution)

    kwargs = tu.build_date_dictionary(date)
    formatted_date = variable.date_template.format(**kwargs)

    lat_series = self.dataset[variable.lat_attribute_name]

    if lat_series[0] > lat_series[-1]:
        logging.debug('switching lat min and max')
        tmp = lat_min
        lat_min = lat_max
        lat_max = tmp
        del tmp

    with dask.config.set(scheduler=self.dask_scheduler):
      if isinstance(variable, MultiLevelVariable):
        tmp_result = self.dataset[variable.netcdf_attribute_name].sel(
                                      time=formatted_date,
                                      level=variable.level,
                                      latitude=slice(lat_min, lat_max),
                                      longitude=slice(lon_min, lon_max))
      else:
        if isinstance(variable, SingleLevelVariable):
          tmp_result = self.dataset[variable.netcdf_attribute_name].sel(
                                      time=formatted_date,
                                      latitude=slice(lat_min, lat_max),
                                      longitude=slice(lon_min, lon_max))
        else:
          msg = f"unsupported direct extraction for variable type '{type(variable)}'"
          logging.error(msg)
          raise Exception(msg)
      result = tmp_result.compute()
      return result

  def close(self):
    try:
      self.dataset.close()
    except :
      pass

  def __enter__(self):
    pass

  def __exit__(self) :
    self.close()


class ExtractionComputedVariable(VariableVisitor):

  def __init__(self, time_series, date, lat, lon, half_lat_frame,
               half_lon_frame):
    self.data_array_mapping = dict()
    self.time_series        = time_series
    self.date               = date
    self.lat                = lat
    self.lon                = lon
    self.half_lat_frame     = half_lat_frame
    self.half_lon_frame     = half_lon_frame
    self.result             = None

  def visit_SingleLevelVariable(self, variable):
    region = self.time_series.extract_square_region(variable, self.date,
                                                    self.lat, self.lon,
                                                    self.half_lat_frame,
                                                    self.half_lon_frame)
    self.data_array_mapping[variable.str_id] = region

  def visit_MultiLevelVariable(self, variable):
    self.visit_SingleLevelVariable(variable)

  def visit_ComputedVariable(self, variable):
    calculator = XarrayRpnCalculator(variable.computation_expression,
                                     self.data_array_mapping)
    logging.debug('starting a xarray RPN calculator')
    self.result = calculator.compute()

"""
import logging
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
"""
def unit_test():
  import os.path as path
  from datetime import datetime
  variable_parent_dir_path = '/home/sgardoll/cyclone/variables'
  str_id = 'msl'
  date = datetime(2000, 10, 1, 0)
  lat = 39.7
  lon = -47.9
  half_lat_frame = 16
  half_lon_frame = 16

  var = Variable.load(path.join(variable_parent_dir_path, f"{str_id}{Variable.FILE_NAME_POSTFIX}"))
  ts = XarrayTimeSeries(var, date)
  subregion = ts.extract_square_region(var, date, lat, lon, half_lat_frame, half_lon_frame)
  print(subregion.shape)

