#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 12:13:52 2019

@author: sebastien@gardoll.fr
"""

from enum import Enum


class TensorDimension(Enum):

    IMG     = 'img'
    MEAN    = 'mean'
    STD     = 'std'
    CHANNEL = 'channel'
    X       = 'x'
    Y       = 'y'
