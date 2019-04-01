#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 28 16:14:08 2019

@author: sebastien@gardoll.fr
"""

def round_nearest(value, resolution, num_decimal):
  return round(round(value / resolution) * resolution, num_decimal)
