#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 5 10:00:00 2020

@author: sebastien@gardoll.fr
"""

import numpy as np
import pandas as pd

from typing import Tuple, Sequence, Mapping, Set

from nxtensor.exceptions import ConfigurationError
from nxtensor.core.types import VariableId, LabelId, Period

import nxtensor.utils.hdf5_utils as hu
import nxtensor.utils.file_utils as fu
import nxtensor.utils.naming_utils as nu
import nxtensor.utils.time_utils as tu
import nxtensor.utils.csv_utils as cu
import nxtensor.utils.db_utils as du

from sklearn.preprocessing import StandardScaler

import os

import os.path as path

import nxtensor.core.xarray_channel_extraction as chan_xtract

PANDAS_CSV_WRITE_OPTS = {k: v for k, v in cu.DEFAULT_CSV_OPTIONS.items()}
PANDAS_CSV_WRITE_OPTS['header'] = True
PANDAS_CSV_WRITE_OPTS['index'] = False


PANDAS_CSV_READ_OPTS = {k: v for k, v in cu.DEFAULT_CSV_OPTIONS.items()}
PANDAS_CSV_READ_OPTS['dtype'] = chan_xtract.METADATA_TYPES


def compute_block_file_structure(blocks_dir_path: str) -> \
        Tuple[Sequence[Period], Sequence[LabelId], Mapping[Period, Mapping[LabelId, Tuple[str, str]]]]:
    # TODO: let the user choose the periods and labels (and implement a file discriminant).
    # File system structure: see naming_utils.
    fs_iter = os.walk(blocks_dir_path)
    next(fs_iter)  # Discard the root of the fs tree (periods in str).
    periods: Set[Period] = set()
    label_ids = set()
    block_file_structure = dict()
    for root, dirs, files in fs_iter:
        if not files:
            current_period_str: str = path.basename(root)
            current_period: Period = tu.create_period(current_period_str)
            periods.add(current_period)
            current_label_ids = dirs
            block_file_structure[current_period] = dict()
            for label_id in current_label_ids:
                label_ids.add(label_id)
                parent_dir = path.join(blocks_dir_path, current_period_str, label_id)
                data_file_path_template, metadata_file_path_template = \
                    nu.compute_data_meta_data_file_template_path(parent_dir)
                block_file_structure[current_period][label_id] = (data_file_path_template, metadata_file_path_template)
    return tu.sort_periods(periods), nu.sort_labels(label_ids), block_file_structure


def count_block_images(variable_id: VariableId,
                       metadata_block_has_header: bool,
                       block_file_structure: Mapping[Period, Mapping[LabelId, Tuple[str, str]]])\
                       -> Tuple[int, Mapping[Period, Mapping[LabelId, Tuple[str, str, int]]]]:
    total_number_images = 0
    annotated_block_file_structure = dict()
    for period, label_mapping in block_file_structure.items():
        annotated_block_file_structure[period] = dict()
        for label_id, label_data in label_mapping.items():
            metadata_file_path = label_data[1].format(variable_id)
            number_images = fu.count_lines_text_file(metadata_file_path) - 1 \
                if metadata_block_has_header else fu.count_lines_text_file(metadata_file_path)
            total_number_images += number_images
            annotated_block_file_structure[period][label_id] = (label_data[0], label_data[1], number_images)

    return total_number_images, annotated_block_file_structure


# It doesn't do anything.
def default_block_processing_func(periods: Sequence[Period],
                                  label_ids: Sequence[LabelId],
                                  block_file_structure: Mapping[Period, Mapping[LabelId,
                                                                                Tuple[np.ndarray, pd.DataFrame, int]]])\
                    -> Tuple[Sequence[Period], Sequence[LabelId],
                             Mapping[Period, Mapping[LabelId, Tuple[np.ndarray, pd.DataFrame, int]]]]:
    return periods, label_ids, block_file_structure


def concatenate_data_compute_dataset_indexes(periods: Sequence[Period],
                                             label_ids: Sequence[LabelId],
                                             total_number_images: int,
                                             block_file_structure: Mapping[Period, Mapping[LabelId,
                                                                           Tuple[np.ndarray, pd.DataFrame, int]]],
                                             ratios: Sequence[Tuple[str, float]]) \
                       -> Tuple[np.ndarray, pd.DataFrame, Sequence[Tuple[str, Sequence[int]]]] :
    # Compute the number of images for each dataset.
    image_ratios = list()
    ratio_sum: float = 0
    image_number_sum: int = 0
    for ratio in ratios:
        ratio_sum += ratio[1]
        number_images: int = round(ratio[1]*total_number_images)
        image_number_sum += number_images
        image_ratios.append((ratio[0], number_images))

    # Check ratio consistency.
    if ratio_sum > 1.:
        raise ConfigurationError("the sum of ratios cannot be over 1")

    # Fix round issues.
    round_diff = image_number_sum - total_number_images
    if round_diff != 0:
        image_ratio = image_ratios[-1]
        image_ratios[-1] = (image_ratio[0], image_ratio[1] - round_diff)

    del round_diff, ratio_sum, ratios, image_number_sum

    # Stack the data & metadata and compute split positions.
    data = list()
    metadata = list()
    dataset_indexes = list()
    dataset_iter = iter(image_ratios)
    current_dataset = next(dataset_iter)
    dataset_image_counter = 0
    left_index = 0
    #print(f'total number imgs: {total_number_images}')
    #print(f'image ratios: {image_ratios}')
    for period in periods:
        #print(f'period: {period}')
        period_data = block_file_structure[period]
        for label_id in label_ids:
            if label_id in period_data:
                label_data = period_data[label_id]
                data.append(label_data[0])
                metadata.append(label_data[1])
                dataset_image_counter += label_data[2]    # this condition is related to the last dataset
        #print(f'img counter: {left_index + dataset_image_counter}')
        if dataset_image_counter >= current_dataset[1] or left_index + dataset_image_counter == total_number_images:
            right_index = left_index + dataset_image_counter
            print(f"> dataset '{current_dataset[0]}' real ratio is {dataset_image_counter*100./total_number_images}%")
            dataset_indexes.append((current_dataset[0], [index for index in range(left_index, right_index)]))
            left_index = right_index
            dataset_image_counter = 0
            try:
                current_dataset = next(dataset_iter)
            except StopIteration:
                current_dataset = None  # Well the first for loop should end here.
    concatenated_data = concatenate_data(data)
    concatenated_metadata = concatenate_metadata(metadata)
    return concatenated_data, concatenated_metadata, dataset_indexes


def concatenate_metadata(metadata: Sequence[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(objs=metadata, ignore_index=True)


def concatenate_data(data: Sequence[np.ndarray]) -> np.ndarray:
    return np.concatenate(data)


def load_data_blocks(variable_id: VariableId, periods: Sequence[Period], label_ids: Sequence[LabelId],
                     block_file_structure: Mapping[Period, Mapping[LabelId, Tuple[str, str, int]]]) \
                     -> Mapping[Period, Mapping[LabelId, Tuple[np.ndarray, pd.DataFrame, int]]]:
    print(f'> loading {variable_id} channel data')
    block_data_structure = dict()
    for period in periods:
        label_data_structure = dict()
        for label_id in label_ids:
            if label_id in block_file_structure[period]:
                data_file_template = block_file_structure[period][label_id][0]
                data = hu.read_ndarray_from_hdf5(data_file_template.format(variable_id))
                metadata_file_template = block_file_structure[period][label_id][1]
                metadata = du.load_csv_file(metadata_file_template.format(variable_id), PANDAS_CSV_READ_OPTS)
                image_number = block_file_structure[period][label_id][2]
                label_data_structure[label_id] = (data, metadata, image_number)
        block_data_structure[period] = label_data_structure
    return block_data_structure


def normalize_scale_with_params(channel_data: np.ndarray, mean: float = None, std: float = None) -> np.ndarray:
    tmp = channel_data.reshape((-1, 1), order='C')
    scaler = StandardScaler(copy=False, with_mean=mean, with_std=std)
    scaler.mean_ = mean
    scaler.scale_ = std
    scaler.transform(tmp)
    std_channel_data = tmp.reshape(channel_data.shape, order='C')
    return std_channel_data


def normalize_scale(channel_data: np.ndarray, with_mean: bool, with_std: bool) -> Tuple:
    tmp = channel_data.reshape((-1, 1), order='C')
    scaler = StandardScaler(copy=False, with_mean=with_mean, with_std=with_std)
    scaler.fit(tmp)
    scaler.transform(tmp)
    std_channel_data = tmp.reshape(channel_data.shape, order='C')
    if with_mean and with_std:
        return std_channel_data, scaler.mean_, scaler.scale_
    elif with_mean:
        return std_channel_data, scaler.mean_
    elif with_std:
        return std_channel_data, scaler.scale_
    else:
        return std_channel_data


def split_channel(channel_data: np.ndarray, channel_metadata: pd.DataFrame, dataset_indexes: Sequence[int]) \
                  -> Tuple[np.ndarray, pd.DataFrame]:
    return channel_data[dataset_indexes], channel_metadata.iloc[dataset_indexes]


def stack_channel(channel_data_list: Sequence[np.ndarray]) -> np.ndarray:
    tensor = np.stack(channel_data_list, axis=3)
    return tensor


def shuffle_data(data: np.ndarray, metadata: pd.DataFrame) -> Tuple[np.ndarray, pd.DataFrame]:
    permutations = np.random.permutation(data.shape[0])
    shuffled_data = data[permutations]
    shuffled_metadata = metadata.iloc[permutations]
    shuffled_metadata.reset_index(drop=True)
    return shuffled_data, shuffled_metadata


def __all_tests():
    pass


if __name__ == '__main__':
    __all_tests()
