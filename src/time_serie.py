#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 27 11:57:37 2019

@author: sebastien@gardoll.fr
"""

from abc import ABC, abstractmethod
import xarray as xr
import logging
from variable import MultiLevelVariable, SingleLevelVariable,
VariableNetcdfFilePathVisitor, VariableVisitor
import coordinate_utils as cu
import time_utils as tu
import re
import xarray_utils as xu

class TimeSerie(ABC):

  pass

class XarrayTimeSerie(TimeSerie):

  DATE_TEMPLATE = "{year}-{month}-{day}T{hour}:{minute}:{second}:{microsecond}"

  # Date is expected to be a datetime instance.
  def __init_(self, variable, date):
    visitor = VariableNetcdfFilePathVisitor(date)
    variable.accept(visitor)
    netcdf_file_path_dict = visitor.result
    self.variables = (variable)
    self.date = date
    self._open_netcdf(netcdf_file_path_dict.values)

  # Options is a dictionary of named parameters for the methods open_mfdataset or
  # open_dataset.
  def _open_netcdf(self, netcdf_file_paths, options = None):
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

  # Extract the region that centers the given lat/lon location.
  def extract_square_region(self, variable, date, lat, lon,
                            half_lat_frame, half_lon_frame):
    variable_type = type(variable)
    if variable_type is MultiLevelVariable or \
       variable_type is SingleLevelVariable:
      XarrayTimeSerie._extract_square_region(self.dataset, variable, date, lat, lon,
                                             half_lat_frame, half_lon_frame)
    else:
      pass

  # Extract the region that centers the given lat/lon location. Variable must be
  # SingleLevelVariable or MultiLevelVariable.
  @staticmethod
  def extract_square_region(dataset, variable, date, lat, lon,
                            half_lat_frame, half_lon_frame):
    rounded_lat = cu.round_nearest(lat, variable.lat_resolution, variable.nb_lat_decimal)
    rounded_lon = cu.round_nearest(lon, variable.lon_resolution, variable.nb_lon_decimal)

    # Minus LAT_RESOLUTION because the upper bound in slice is included.
    lat_min  = (rounded_lat - half_lat_frame + variable.lat_resolution)
    lat_max  = (rounded_lat + half_lat_frame)
    # Minus LON_RESOLUTION because the upper bound in slice is included.
    lon_min  = (rounded_lon - half_lon_frame)
    lon_max  = (rounded_lon + half_lon_frame - variable.lon_resolution)

    kwargs = tu.build_date_dictionary(date)
    formatted_date = XarrayTimeSerie.DATE_TEMPLATE.format(**kwargs)

    if type(variable) is MultiLevelVariable:
      tmp_result = dataset.sel(time=formatted_date,
                               level=variable.level,
                               latitude=slice(lat_max, lat_min),
                               longitude=slice(lon_min, lon_max))
    else:
      if type(variable) is SingleLevelVariable:
        tmp_result = dataset.sel(time=formatted_date,
                                 latitude=slice(lat_max, lat_min),
                                 longitude=slice(lon_min, lon_max))
      else:
        msg = 'cannot extract directly computed variables'
        logging.error(msg)
        raise Exception(msg)

    # Drop netcdf behavior so as to stack the subregions without Na filling
    # (concat netcdf default behavior is to concatenate the subregions on a
    # wilder region that includes all the subregions).
    result = xr.DataArray(data=tmp_result.to_array().data)
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


class ExtractionComputeVariable(VariableVisitor):

  def __init__(self, dataset, variable, date, lat, lon, half_lat_frame,
               half_lon_frame):
    self.result         = dict()
    self.dataset        = dataset
    self.date           = date
    self.lat            = lat
    self.lon            = lon
    self.half_lat_frame = half_lat_frame
    self.half_lon_frame = half_lon_frame

  def visit_SingleLevelVariable(self, variable):
    region = XarrayTimeSerie.extract_square_region(self.dataset, variable,
                                                   self.date, self.lat, self.lon,
                                                   self.half_lat_frame,
                                                   self.half_lon_frame)
    self.result[variable.str_id] = region

  def visit_MultiLevelVariable(self, variable):
    self.visit_SingleLevelVariable(variable)

  def visit_ComputedVariable(self, variable):
    todo

