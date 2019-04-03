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

  DECREASING_DEGREE_NORTH = 'decreasing_degree_north'
  AMERICAN_DEGREE_EAST    = 'american_degree_east'

class DbFormat(Enum):
  CSV = 'csv'

class RegionGeometry(Enum):
  SQUARE = 'square'
