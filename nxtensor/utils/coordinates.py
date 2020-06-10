#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 28 16:14:08 2019

@author: sebastien@gardoll.fr
"""


class Coordinate:

    LAT = 'lat'
    LON = 'lon'


class CoordinateFormat:

    DECREASING_DEGREE_NORTH  = 'decreasing_degree_north'   # From   90° to  -90°
    INCREASING_DEGREE_NORTH  = 'increasing_degree_north'   # From  -90° to   90°
    ZERO_TO_360_DEGREE_EAST  = 'zero_to_360_degree_east'   # From    0° to  360°
    M_180_TO_180_DEGREE_EAST = 'm_180_to_180_degree_east'  # From -180° to  180°
    UNKNOWN                  = 'unknown'
