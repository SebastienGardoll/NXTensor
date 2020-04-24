# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 12:00:00 2020

@author: sebastien@gardoll.fr
"""

from enum import Enum


class Coordinate(Enum):

    LAT = 'lat'
    LON = 'lon'


def round_nearest(value: float, resolution: float, num_decimal: int) -> float:
    return round(round(value / resolution) * resolution, num_decimal)
