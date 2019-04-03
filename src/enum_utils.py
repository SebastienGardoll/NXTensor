#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 12:13:52 2019

@author: sebastien@gardoll.fr
"""

from enum import Enum

class TimeResolution(Enum):

  YEAR      = 'year'
  MONTH     = 'month'
  DAY       = 'day'
  SIX_HOURS = 'six_hours'
  HOUR      = 'hour'
  MINUTE    = 'minute'
  SECOND    = 'second'

class CoordinateFormat(Enum):

  DECREASING_DEGREE_NORTH = 'decreasing_degree_north' # From   90° to  -90°
  INCREASING_DEGREE_NORTH = 'increasing_degree_north' # From  -90° to   90°
  AMERICAN_DEGREE_EAST    = 'american_degree_east'    # From    0° to  360°
  EUROPEAN_DEGREE_EAST    = 'european_degree_east'    # From -180° to  180°

class DbFormat(Enum):
  CSV = 'csv'

class SelectionShape(Enum):
  SQUARE = 'square'
