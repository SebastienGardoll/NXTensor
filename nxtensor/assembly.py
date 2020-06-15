#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 5 10:00:00 2020

@author: sebastien@gardoll.fr
"""

import pickle

import numpy as np
import pandas as pd

from typing import Tuple, Sequence, Mapping, Callable

from nxtensor.core.types import VariableId, LabelId, Period

import nxtensor.utils.hdf5_utils as hu
import nxtensor.utils.naming_utils as nu
import nxtensor.utils.csv_utils as cu
import nxtensor.utils.db_utils as du

from multiprocessing import Pool

import nxtensor.core.assembly as assembly

import os
import os.path as path

from nxtensor.exceptions import ConfigurationError
from nxtensor.extraction import ExtractionConfig
from nxtensor.utils.tensor_dimensions import TensorDimension

import random


def preprocessing(extraction_conf_file_path: str) -> None:
    extraction_conf = ExtractionConfig.load(extraction_conf_file_path)
    print(f"> starting pre-process of assemble '{extraction_conf.str_id}'")
    metadata_block_has_header = True
    preprocessing_file_path = __generate_preprocessing_file_path(extraction_conf)
    periods, label_ids, block_file_structure = assembly.compute_block_file_structure(extraction_conf.blocks_dir_path)

    if extraction_conf.has_tensor_to_be_shuffled:
        periods = random.sample(periods, len(periods))

    variable_id = next(iter(extraction_conf.get_variables().keys()))
    total_number_images, block_file_structure = assembly.count_block_images(variable_id,
                                                                            metadata_block_has_header,
                                                                            block_file_structure)
    os.makedirs(path.dirname(preprocessing_file_path), exist_ok=True)
    try:
        with open(preprocessing_file_path, 'wb') as file:
            pickle.dump(obj=(periods, label_ids, total_number_images, block_file_structure),
                        file=file, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as e:
        msg = f'> [ERROR] unable to save assembly preprocessing in {preprocessing_file_path}'
        raise Exception(msg, e)
    print('> pre-process is completed')


def __generate_preprocessing_file_path(extraction_conf: ExtractionConfig) -> str:
    return nu.compute_preprocessing_file_path(extraction_conf.str_id, 'assembly', extraction_conf.tmp_dir_path)


def __load_extraction_conf(extraction_conf_file_path: str):
    try:
        return ExtractionConfig.load(extraction_conf_file_path)
    except Exception as e:
        msg = f'> [ERROR] unable to load extraction conf file located at {extraction_conf_file_path}'
        raise ConfigurationError(msg, e)


def channel_building_batch(extraction_conf_file_path: str, nb_workers: int = None,
                           user_specific_block_processing:
                               Callable[[Sequence[Period], Sequence[LabelId],
                                         Mapping[Period, Mapping[LabelId, Tuple[np.ndarray, pd.DataFrame, int]]]],
                                        Tuple[Sequence[Period], Sequence[LabelId],
                                        Mapping[Period, Mapping[LabelId, Tuple[np.ndarray, pd.DataFrame, int]]]]] =
                               assembly.default_block_processing_func) -> None:
    extraction_conf: ExtractionConfig = ExtractionConfig.load(extraction_conf_file_path)
    preprocessing_file_path = __generate_preprocessing_file_path(extraction_conf)
    # Deserialize the preprocessing data.
    try:
        with open(preprocessing_file_path, 'rb') as file:
            periods, label_ids, total_number_images, block_file_structure = pickle.load(file=file)
    except Exception as e:
        msg = f'> [ERROR] unable to load assembly pre-processing located at {preprocessing_file_path}'
        raise Exception(msg, e)

    variable_ids = list(extraction_conf.get_variables().keys())

    static_parameters = (extraction_conf.channels_dir_path, periods, label_ids,
                         total_number_images, block_file_structure, extraction_conf.tensor_dataset_ratios,
                         user_specific_block_processing)
    parameters_list = [(variable_id, *static_parameters) for variable_id in variable_ids]

    len_variable_ids = len(variable_ids)
    if not nb_workers:
        nb_workers = len_variable_ids

    if nb_workers > 1:
        if nb_workers > len_variable_ids:
            nb_workers = len_variable_ids
        print(f"> assembling the channels '{nu.list_to_string(variable_ids)}' in parallel (nb worker: {nb_workers})")
        with Pool(processes=nb_workers) as pool:
            pool.map(func=__map_channel_building, iterable=parameters_list, chunksize=1)
    else:
        print(f"> assembling the channels '{nu.list_to_string(variable_ids)}' sequentially")
        for parameters in parameters_list:
            __map_channel_building(parameters)


def __map_channel_building(parameters):
    channel_building(*parameters)


def channel_building(variable_id: VariableId, channel_output_dir_path: str,
                     periods: Sequence[Period], label_ids: Sequence[LabelId], total_number_images: int,
                     block_file_structure: Mapping[Period, Mapping[LabelId, Tuple[str, str, int]]],
                     ratios: Mapping[str, float],
                     user_specific_block_processing:
                         Callable[[Sequence[Period], Sequence[LabelId],
                                   Mapping[Period, Mapping[LabelId, Tuple[np.ndarray, pd.DataFrame, int]]]],
                                  Tuple[Sequence[Period], Sequence[LabelId],
                                  Mapping[Period, Mapping[LabelId, Tuple[np.ndarray, pd.DataFrame, int]]]]] =
                         assembly.default_block_processing_func) -> None:
    block_data_structure = assembly.load_data_blocks(variable_id, periods, label_ids, block_file_structure)
    periods, label_ids, block_data_structure = user_specific_block_processing(periods, label_ids, block_data_structure)
    channel_data, channel_metadata, dataset_indexes = \
        assembly.concatenate_data_compute_dataset_indexes(periods, label_ids, total_number_images, block_data_structure,
                                                          ratios)
    channel_data, mean, scale = assembly.normalize_scale(channel_data, True, True)
    os.makedirs(channel_output_dir_path, exist_ok=True)
    # Split the data and metadata according to the user's dataset specifications.
    for dataset_name, indexes in dataset_indexes:
        dataset_data, dataset_metadata = assembly.split_channel(channel_data, channel_metadata, indexes)
        dataset_data_file_path, dataset_metadata_file_path = \
            nu.compute_data_meta_data_file_path(variable_id, channel_output_dir_path, dataset_name)
        hu.write_ndarray_to_hdf5(dataset_data_file_path, dataset_data)
        du.save_to_csv_file(dataset_metadata, dataset_metadata_file_path, assembly.PANDAS_CSV_WRITE_OPTS)
    
    min = channel_data.min()
    max = channel_data.max()
    stat_data = [{TensorDimension.MEAN: mean[0], TensorDimension.STD: scale[0], TensorDimension.MIN: min,
                  TensorDimension.MAX: max}]
    stat_file_path = nu.compute_stat_file_path(variable_id, channel_output_dir_path)
    cu.to_csv(data=stat_data, file_path=stat_file_path)


def channel_stacking_batch(tensor_id: str,
                           extraction_conf_file_path: str,
                           nb_workers: int = None) -> None:

    extraction_conf: ExtractionConfig = ExtractionConfig.load(extraction_conf_file_path)
    dataset_names = extraction_conf.tensor_dataset_ratios.keys()
    variable_ids = list(extraction_conf.get_variables().keys())
    static_parameters = (tensor_id, extraction_conf.tensors_dir_path, extraction_conf.channels_dir_path,
                         variable_ids, extraction_conf.has_tensor_to_be_shuffled)
    parameters_list = [(dataset_name, *static_parameters) for dataset_name in dataset_names]

    len_dataset_types = len(dataset_names)
    if not nb_workers:
        nb_workers = len_dataset_types

    if nb_workers > 1:
        if nb_workers > len_dataset_types:
            nb_workers = len_dataset_types

        print(f"> stacking the dataset '{nu.list_to_string(dataset_names)}' in parallel (nb worker: {nb_workers})")
        with Pool(processes=nb_workers) as pool:
            pool.map(func=__map_channel_stacking, iterable=parameters_list, chunksize=1)
    else:
        print(f"> stacking the dataset '{nu.list_to_string(dataset_names)}' sequentially")
        for parameters in parameters_list:
            channel_stacking(*parameters)


def __map_channel_stacking(parameters):
    channel_stacking(*parameters)


def channel_stacking(dataset_name: str, tensor_id: str, tensor_output_dir: str,
                     channels_dir: str, variable_ids: Sequence[VariableId], has_to_shuffle: bool) -> Tuple[str, str]:
    channel_data_list = list()
    channel_metadata_file_path = ''
    for variable_id in variable_ids:
        print(f"> loading the data and metadata of variable '{variable_id}'")
        channel_data_file_path, channel_metadata_file_path = \
            nu.compute_data_meta_data_file_path(variable_id, channels_dir, dataset_name)
        channel_data = hu.read_ndarray_from_hdf5(channel_data_file_path)
        channel_data_list.append(channel_data)

    print("> stacking the channels")
    tensor_data = assembly.stack_channel(channel_data_list)
    metadata = du.load_csv_file(channel_metadata_file_path, assembly.PANDAS_CSV_READ_OPTS)

    if has_to_shuffle:
        print(f"> shuffling the tensor '{dataset_name}'")
        tensor_data, metadata = assembly.shuffle_data(tensor_data, metadata)

    tensor_data_file_path, tensor_metadata_file_path = \
        nu.compute_data_meta_data_file_path(tensor_id, tensor_output_dir, dataset_name)

    print(f"> saving the tensor '{dataset_name}' data and metadata")
    os.makedirs(tensor_output_dir, exist_ok=True)
    du.save_to_csv_file(metadata, tensor_metadata_file_path, assembly.PANDAS_CSV_WRITE_OPTS)
    hu.write_ndarray_to_hdf5(tensor_data_file_path, tensor_data)
    return tensor_data_file_path, tensor_metadata_file_path


def __test_preprocess(extraction_conf_file_path: str) -> None:
    preprocessing(extraction_conf_file_path)


def __test_channel_building_batch(extraction_conf_file_path: str) -> None:
    channel_building_batch(extraction_conf_file_path)


def __test_channel_stacking_batch(extraction_conf_file_path: str) -> None:
    tensor_id = '2000_10'
    channel_stacking_batch(tensor_id, extraction_conf_file_path)


def __all_tests():
    config_dir_path = '/home/sgardoll/extraction_config'
    extraction_conf_file_path = path.join(config_dir_path, '2000_10_extraction_config.yml')
    __test_preprocess(extraction_conf_file_path)
    __test_channel_building_batch(extraction_conf_file_path)
    __test_channel_stacking_batch(extraction_conf_file_path)


if __name__ == '__main__':
    __all_tests()
