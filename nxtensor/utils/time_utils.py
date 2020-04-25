#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 28 12:04:09 2019

@author: sebastien@gardoll.fr
"""

import datetime
import logging
from typing import Dict, Sequence, Union, Mapping

from nxtensor.utils.time_resolutions import TimeResolution


def build_date_dictionary(date: datetime.datetime) -> Mapping[str, Union[str, int]]:
    if type(date) is datetime.datetime:
        return {'year': date.year, 'month': date.month, 'month2d': f"{date.month:02d}",
                'day': date.day, 'hour': date.hour, 'minute': date.minute,
                'second': date.second, 'microsecond': date.microsecond}
    else:
        msg = f"the given date '{date}', is not an instance of datetime"
        logging.error(msg)
        raise Exception(msg)


def from_time_list_to_dict(time_list: Sequence[int]) -> Dict[TimeResolution, Union[str, int]]:
    # Time_list is a list that contains the value of the TimeResolution::TIME_RESOLUTION_KEYS
    # (see tensor_dimensions.py).
    # We cannot instantiate a date without the day number. That's why this function
    # was created. The list must have the same order than the TimeResolution::TIME_RESOLUTION_KEYS .
    # Python 3.7 dict preserves the insertion order.
    result: Dict[TimeResolution, Union[str, int]] = dict()

    list_len = len(time_list)
    if list_len >= 1:
        result[TimeResolution.YEAR]        = time_list[0]

    if list_len >= 2:
        result[TimeResolution.MONTH]       = time_list[1]
        result[TimeResolution.MONTH2D]     = f"{time_list[1]:02d}"

    if list_len >= 3:
        result[TimeResolution.DAY]         = time_list[2]
        result[TimeResolution.DAY2D]       = f"{time_list[2]:02d}"

    if list_len >= 4:
        result[TimeResolution.HOUR]        = time_list[3]
        result[TimeResolution.HOUR2D]      = f"{time_list[3]:02d}"

    if list_len >= 5:
        result[TimeResolution.MINUTE]      = time_list[4]

    if list_len >= 6:
        result[TimeResolution.SECOND]      = time_list[5]

    if list_len >= 7:
        result[TimeResolution.MILLISECOND] = time_list[6]

    if list_len >= 8:
        result[TimeResolution.MICROSECOND] = time_list[7]

    return result


def remove_2d_time_dict(time_dict: Dict[str, str]) -> Dict[str, str]:
    time_dict.pop('month2d', None)
    time_dict.pop('hour2d', None)
    return time_dict
