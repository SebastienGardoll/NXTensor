#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 28 12:04:09 2019

@author: sebastien@gardoll.fr
"""

import datetime
import logging

def build_date_dictionary(date):
  if type(date) is datetime.datetime:
    return {'year': date.year, 'month': date.month, 'month2d': f"{date.month:02d}",
            'day': date.day, 'hour': date.hour, 'minute': date.minute,
            'second': date.second, 'microsecond': date.microsecond}
  else:
    msg = f"the given date '{date}', is not an instance of datetime"
    logging.error(msg)
    raise Exception(msg)

# Time_list is a list that contains the value of the TimeKey'KEYS (see enum_utils.py).
# We cannot instantiate a date without the day number. That's why this function
# was created.
# Python 3.7 dict preserves order.
def from_time_list_to_dict(time_list):
  list_len = len(time_list)
  result = dict()
  if list_len >= 1:
    result['year']        = time_list[0]

  if list_len >= 2:
    result['month']       = time_list[1]
    result['month2d']     = f"{time_list[1]:02d}"

  if list_len >= 3:
    result['day']         = time_list[2]

  if list_len >= 4:
    result['hour']        = time_list[3]
    result['hour2d']      = f"{time_list[3]:02d}"

  if list_len >= 5:
    result['minute']      = time_list[4]

  if list_len >= 6:
    result['second']      = time_list[5]

  if list_len >= 7:
    result['microsecond'] = time_list[6]

  return result

def remove_2d_time_dict(time_dict):
  time_dict.pop('month2d', None)
  time_dict.pop('hour2d', None)
  return time_dict