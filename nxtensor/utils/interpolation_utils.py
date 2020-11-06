#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  4 10:22:43 2020

@author: Sébastien Gardoll
"""
from typing import Tuple

from scipy.interpolate import griddata

import numpy as np

import nxtensor.utils.hdf5_utils as hu

from concurrent.futures import ThreadPoolExecutor


def interpolate_tensor(tensor_file_path: str, src_x_resolution: float, src_y_resolution: float,
                       dest_x_resolution: float, dest_y_resolution: float, method: str, is_channels_last: bool,
                       nb_processes: int) -> np.ndarray:
    # Tensor must be of shape: (image, channel, x, y)
    # The dimensions of the tensor can be swapped: see has_to_swap_channel.
    tensor = hu.read_ndarray_from_hdf5(tensor_file_path)
    # The view of the tensor is channels first for the rest of the algorithm.
    if is_channels_last:
        tensor = tensor.swapaxes(1, 3).swapaxes(2, 3)
    x_size = tensor.shape[2]
    y_size = tensor.shape[3]
    nb_channels = tensor.shape[1]
    nb_images = tensor.shape[0]
    channel_shape = (x_size, y_size)
    src_grid = __compute_2d_grid_coordinates(x_size, src_x_resolution, y_size, src_y_resolution)
    x_resolution_factor = (src_x_resolution - dest_x_resolution) * 2.
    y_resolution_factor = (src_y_resolution - dest_y_resolution) * 2.
    dest_grid = __compute_2d_grid_coordinates(x_size, dest_x_resolution, y_size, dest_y_resolution,
                                              x_resolution_factor, y_resolution_factor)
    image_range = range(0, nb_images)
    if nb_processes > 1:
        result = np.ndarray(shape=(nb_images, nb_channels, x_size, y_size), dtype=float)
        static_parameters = (tensor, src_grid, dest_grid, channel_shape, method, is_channels_last, result)
        parameters_list = [(image_index, *static_parameters) for image_index in image_range]
        with ThreadPoolExecutor(max_workers=nb_processes) as executor:
            executor.map(__map_interpolated_image, parameters_list)
        if is_channels_last:
            result = result.swapaxes(1, 3).swapaxes(1, 2)
        return result
    else:
        interpolated_images = list()
        for image_index in image_range:
            interpolated_image = interpolate_image(image_index, tensor, src_grid, dest_grid, channel_shape, method,
                                                   is_channels_last)
            interpolated_images.append(interpolated_image)
        return np.stack(interpolated_images)


def __map_interpolated_image(parameter_list):
    interpolate_image(*parameter_list)


def interpolate_image(image_index: int, tensor: np.ndarray, src_grid: np.ndarray, dest_grid: np.ndarray,
                      channel_shape: Tuple[int, int], method: str, is_channel_last: bool,
                      dest_buffer: np.ndarray = None) -> np.ndarray:
    img = tensor[image_index]
    interpolated_channels = None
    if dest_buffer is None:
        interpolated_channels = list()
    for channel_index in range(0, tensor.shape[1]):  # For each channel of an image.
        channel = img[channel_index]
        interpolated_channel = process_channel(channel, src_grid, dest_grid, channel_shape, method)
        if dest_buffer is None:
            interpolated_channels.append(interpolated_channel)
        else:
            np.copyto(dst=dest_buffer[image_index][channel_index], src=interpolated_channel, casting='no')
    result = None
    if dest_buffer is None:
        if is_channel_last:
            result = np.stack(interpolated_channels, axis=2)
        else:
            result = np.stack(interpolated_channels)
    return result


def process_channel(channel: np.ndarray, src_grid: np.ndarray, dest_grid: np.ndarray, channel_shape: Tuple[int, int],
                    method: str) -> np.ndarray:
    flatten_interpolated_channel = griddata(points=src_grid, values=channel.flatten(), xi=dest_grid, method=method)
    return flatten_interpolated_channel.reshape(channel_shape)


def __compute_1d_grid_coordinates(size: int, resolution: float, downsize_factor: float = 1.) -> np.ndarray:
    last_coordinate = int(size*resolution)
    first_coordinate = 0
    if downsize_factor == 1.:
        offset = 0
    else:
        offset = int(last_coordinate*downsize_factor)
    return np.arange(first_coordinate+offset, last_coordinate+offset, resolution)


def __compute_2d_grid_coordinates(x_size: int, x_resolution: float, y_size: int, y_resolution: float,
                                  x_downsize_factor: float = 1., y_downsize_factor: float = 1.) -> np.ndarray:
    x_grid = __compute_1d_grid_coordinates(x_size, x_resolution, x_downsize_factor)
    y_grid = __compute_1d_grid_coordinates(y_size, y_resolution, y_downsize_factor)
    return np.dstack(np.meshgrid(x_grid, y_grid)).reshape(-1, 2)


def __all_tests():

    import time
    import os.path as path
    import nxtensor.utils.time_utils as tu
    import nxtensor.utils.image_utils as iu

    # tensor_file_path = '/data/sgardoll/merra2_extractions/2010_extraction/tensors/training_2010_data.h5'
    # DEBUG
    tensor_file_path = '/home_local/sgardoll/tmp_cyclone/tensors/training_2010_data.h5'
    src_x_resolution = 0.5
    src_y_resolution = 0.625
    dest_x_resolution = 0.25
    dest_y_resolution = 0.25
    method = 'linear'
    nb_process = 4
    is_channels_last = True
    start = time.time()
    interpolated_tensor = interpolate_tensor(tensor_file_path, src_x_resolution, src_y_resolution, dest_x_resolution,
                                             dest_y_resolution, method, is_channels_last, nb_process)
    tensor_parent_dir_path = path.dirname(tensor_file_path)
    tensor_filename = path.basename(tensor_file_path)
    interpolated_tensor_file_path = path.join(tensor_parent_dir_path, f'interpolated_{tensor_filename}')
    hu.write_ndarray_to_hdf5(interpolated_tensor_file_path, interpolated_tensor)
    stop = time.time()
    print(f'> elapsed time: {tu.display_duration(stop-start)}')
    merra2_variable_names = ('slp', 'tqv', 'u10m', 'v10m', 't200', 't500', 'u850', 'v850')
    print(f'> interpolated tensor shape: {interpolated_tensor.shape}')
    iu.display_channels(interpolated_tensor[0], merra2_variable_names, 'title', is_channels_last)


if __name__ == '__main__':
    __all_tests()
