#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  22 17:06:18 2020

@author: sebastien@gardoll.fr
"""
from collections import Sequence, Mapping
import csv
import numpy as np
import h5py

from nxtensor.utils.csv_option_names import CsvOptName

NAME_SEPARATOR: str = '_'


def write_ndarray_to_hdf5(file_path: str, ndarray: np.ndarray) -> None:
    hdf5_file = h5py.File(file_path, 'w')
    hdf5_file.create_dataset('dataset', data=ndarray)
    hdf5_file.close()


def read_ndarray_from_hdf5(file_path: str) -> np.ndarray:
    hdf5_file = h5py.File(file_path, 'r')
    data = hdf5_file.get('dataset')
    return np.array(data)


DEFAULT_CSV_OPTIONS: Mapping[CsvOptName, str] = {CsvOptName.SEPARATOR: ',',
                                                 CsvOptName.LINE_TERMINATOR: '\\n',
                                                 CsvOptName.QUOTE_CHAR: '"',
                                                 CsvOptName.QUOTING: csv.QUOTE_NONNUMERIC,
                                                 CsvOptName.ENCODING: 'utf-8'}


def to_csv(data: Sequence[Mapping[str, any]], file_path: str,
           csv_options: Mapping[CsvOptName, str] = DEFAULT_CSV_OPTIONS) -> None:

    if CsvOptName.ENCODING in csv_options or\
       CsvOptName.LINE_TERMINATOR in csv_options or\
       CsvOptName.SEPARATOR in csv_options:
        csv_options = {k: v for k, v in csv_options.items()}
        if CsvOptName.ENCODING in csv_options:
            encoding = csv_options.pop(CsvOptName.ENCODING)
            file = open(file_path, 'w', encoding=encoding)
        else:
            file = open(file_path, 'w')

        if csv_options[CsvOptName.LINE_TERMINATOR] == DEFAULT_CSV_OPTIONS[CsvOptName.LINE_TERMINATOR]:
            csv_options[CsvOptName.LINE_TERMINATOR] = '\n'

        if CsvOptName.SEPARATOR in csv_options:
            separator = csv_options.pop(CsvOptName.SEPARATOR)
            csv_options['delimiter'] = separator

        header = sorted(data[0].keys())
        csv_writer = csv.writer(file, **csv_options)
        csv_writer.writerow(header)
        for mapping in data:
            ordered_values = [mapping[key] for key in header]
            csv_writer.writerow(ordered_values)
        file.close()
