#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 12:13:52 2019

@author: sebastien@gardoll.fr
"""

from enum import Enum

class TimeResolution(Enum):

  YEAR    = 'year'
  MONTH   = 'month'
  DAY     = 'day'
  SIX_HOURS = 'six_hours'
  HOUR    = 'hour'
  MINUTE  = 'minute'
  SECOND  = 'second'


class CoordinateFormat(Enum):

  SIGNED_DEGREE_NORTH = 'signed_degrees_north'
  SIGNED_DEGREE_SOUTH = 'signed_degrees_south'
  SIGNED_DEGREE_EAST  = 'signed_degrees_east'
  SIGNED_DEGREE_WEST  = 'signed_degrees_west'
