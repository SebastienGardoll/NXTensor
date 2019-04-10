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
def from_time_list_to_date(time_list, resolution_degree):
  pass