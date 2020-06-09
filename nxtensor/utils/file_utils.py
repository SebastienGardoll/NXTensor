#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  22 17:06:18 2020

@author: sebastien@gardoll.fr
"""

CSV_FILE_EXTENSION    = 'csv'
NETCDF_FILE_EXTENSION = 'nc'
HDF5_FILE_EXTENSION   = 'h5'
PICKLE_FILE_EXTENSION = 'pkl'


# Count the every single new line in a text file (included the last one).
def count_lines_text_file(text_file_path: str, read_buffer_size: int = 1024 * 1024) -> int:
    result = 0
    with open(text_file_path, 'rb') as file:
        buffer = file.raw.read(read_buffer_size)
        while buffer:
            result += buffer.count(b'\n')
            buffer = file.raw.read(read_buffer_size)
    return result
