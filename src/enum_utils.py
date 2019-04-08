#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 12:13:52 2019

@author: sebastien@gardoll.fr
"""

class TimeResolution:

  YEAR      = 'year'
  MONTH     = 'month'
  DAY       = 'day'
  HOUR      = 'hour'
  MINUTE    = 'minute'
  SECOND    = 'second'

class CoordinateFormat:

  DECREASING_DEGREE_NORTH = 'decreasing_degree_north' # From   90° to  -90°
  INCREASING_DEGREE_NORTH = 'increasing_degree_north' # From  -90° to   90°
  AMERICAN_DEGREE_EAST    = 'american_degree_east'    # From    0° to  360°
  EUROPEAN_DEGREE_EAST    = 'european_degree_east'    # From -180° to  180°

class DbFormat:

  CSV = 'csv'

class CsvKey:

  SEPARATOR = 'separator'
  HEADER    = 'header_line_number'
  NA_SYMBOL = 'na_symbol'

class SelectionShape:

  SQUARE = 'square'

class TimeKey:

  YEAR   = TimeResolution.YEAR
  MONTH  = TimeResolution.MONTH
  DAY    = TimeResolution.DAY
  HOUR   = TimeResolution.HOUR
  MINUTE = TimeResolution.MINUTE
  SECOND = TimeResolution.SECOND

  KEYS   = (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)

class CoordinateKey:

  LAT   = 'lat'
  LON   = 'lon'

  KEYS  = (LAT, LON)

class CoordinatePropertyKey:

  FORMAT     = 'format'
  RESOLUTION = 'resolution'
  NB_DECIMAL = 'nb_decimal'
