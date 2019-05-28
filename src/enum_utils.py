#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 12:13:52 2019

@author: sebastien@gardoll.fr
"""

class TimeResolution:

  YEAR         = 'year'
  MONTH        = 'month'
  DAY          = 'day'
  HOUR         = 'hour'
  MINUTE       = 'minute'
  SECOND       = 'second'
  MICRO_SECOND = 'microsecond'

class CoordinateFormat:

  DECREASING_DEGREE_NORTH  = 'decreasing_degree_north'  # From   90° to  -90°
  INCREASING_DEGREE_NORTH  = 'increasing_degree_north'  # From  -90° to   90°
  ZERO_TO_360_DEGREE_EAST  = 'zero_to_360_degree_east'  # From    0° to  360°
  M_180_TO_180_DEGREE_EAST = 'm_180_to_180_degree_east' # From -180° to  180°
  UNKNOWN                  = 'unknown'

class DbFormat:

  CSV = 'csv'

class CsvKey:

  SEPARATOR       = 'separator'
  HEADER          = 'header_line_number'
  NA_SYMBOL       = 'na_symbol'
  LINE_TERMINATOR = 'line_terminator'
  ENCODING        = 'encoding'

class SelectionShape:

  SQUARE = 'square'

class TimeKey:

  YEAR         = TimeResolution.YEAR
  MONTH        = TimeResolution.MONTH
  DAY          = TimeResolution.DAY
  HOUR         = TimeResolution.HOUR
  MINUTE       = TimeResolution.MINUTE
  SECOND       = TimeResolution.SECOND
  MICRO_SECOND = TimeResolution.MICRO_SECOND

  KEYS   = (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MICRO_SECOND)

class CoordinateKey:

  LAT   = 'lat'
  LON   = 'lon'

  KEYS  = (LAT, LON)

class DBMetadata:
  LABEL = 'label'
  LAT   = CoordinateKey.LAT
  LON   = CoordinateKey.LON

class CoordinatePropertyKey:

  FORMAT           = 'format'
  RESOLUTION       = 'resolution'
  NB_DECIMAL       = 'nb_decimal'
  NETCDF_ATTR_NAME = 'netcdf_attr_name'

class TensorKey:

  IMG     = 'img'
  MEAN    = 'mean'
  STD     = 'std'
  CHANNEL = 'channel'
