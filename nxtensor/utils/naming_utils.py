from typing import Tuple, Iterable, Sequence

from nxtensor.core.types import LabelId, Period

import functools

import nxtensor.utils.file_utils as fu

import os.path as path


# extraction_id
#     |
#     |_ channel_id
#     |     |_ data and metadata variable channel files (see naming_utils for naming convention)
#     |
#     |_ tensor_id
#     |     |_ data and metadata variable tensor files (see naming_utils for naming convention)
#     |
#     |_ blocks
#          |_ extraction_id
#                  |_ period #1
#                        |_ label #1
#                              |_ data & metadata variable block files (see naming_utils for naming convention)


NAME_SEPARATOR: str = '_'

# {} stands for the str_id of the variable.
__DATA_BLOCK_FILENAME_TEMPLATE: str = '{}.' + fu.HDF5_FILE_EXTENSION
__METADATA_BLOCK_FILENAME_TEMPLATE: str = '{}.' + fu.CSV_FILE_EXTENSION
__STAT_FILENAME_TEMPLATE: str = '{}_stats.' + fu.CSV_FILE_EXTENSION


def compute_data_meta_data_file_path(str_id: str, parent_dir_path: str, *other_filename_prefixes: str)\
        -> Tuple[str, str]:
    data_file_template_path, metadata_file_template_path = \
        compute_data_meta_data_file_template_path(parent_dir_path, *other_filename_prefixes)
    return data_file_template_path.format(str_id), metadata_file_template_path.format(str_id)


def compute_data_meta_data_file_template_path(parent_dir_path: str, *other_filename_prefixes: str) -> Tuple[str, str]:
    if other_filename_prefixes:
        filename_prefix = functools.reduce(lambda x, y: f'{x}{NAME_SEPARATOR}{y}', other_filename_prefixes)
        data_filename_template = f"{filename_prefix}{NAME_SEPARATOR}{__DATA_BLOCK_FILENAME_TEMPLATE}"
        metadata_filename_template = f"{filename_prefix}{NAME_SEPARATOR}{__METADATA_BLOCK_FILENAME_TEMPLATE}"
    else:
        data_filename_template = __DATA_BLOCK_FILENAME_TEMPLATE
        metadata_filename_template = __METADATA_BLOCK_FILENAME_TEMPLATE
    data_file_path_template = path.join(parent_dir_path, data_filename_template)
    metadata_file_path_template = path.join(parent_dir_path, metadata_filename_template)
    return data_file_path_template, metadata_file_path_template


def compute_stat_file_path(str_id: str, parent_dir_path: str, *other_filename_prefixes: str) -> str:
    if other_filename_prefixes:
        filename_prefix = functools.reduce(lambda x, y: f'{x}{NAME_SEPARATOR}{y}', other_filename_prefixes)
        stat_filename_template = f"{filename_prefix}{NAME_SEPARATOR}{__STAT_FILENAME_TEMPLATE}"
    else:
        stat_filename_template = __STAT_FILENAME_TEMPLATE
    stat_file_path_template = path.join(parent_dir_path, stat_filename_template)
    return stat_file_path_template.format(str_id)


def create_period_str(period: Period) -> str:
    return functools.reduce(lambda x, y: f'{x}{NAME_SEPARATOR}{y}', period)


# Sort the labels alphabetically.
def sort_labels(label_ids: Iterable[LabelId]) -> Sequence[LabelId]:
    return sorted(label_ids)
