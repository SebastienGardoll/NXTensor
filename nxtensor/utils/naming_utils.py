from typing import Tuple, Iterable, Sequence

from nxtensor.core.types import LabelId, Period

import functools

import nxtensor.utils.file_utils as fu

import os.path as path


# extraction_id
#     |
#     |_ channels
#     |     |_ data and metadata variable channel files (see naming_utils for naming convention)
#     |
#     |_ tensors
#     |     |_ data and metadata dataset tensor files (see naming_utils for naming convention)
#     |
#     |_ blocks
#          |_ extraction_id
#                  |_ period #1
#                        |_ label #1
#                              |_ data & metadata variable block files (see naming_utils for naming convention)


NAME_SEPARATOR: str = '_'

# {} stands for the str_id of the variable.
__DATA_BLOCK_FILENAME_TEMPLATE: str = '{}' + NAME_SEPARATOR + 'data.' + fu.HDF5_FILE_EXTENSION
__METADATA_BLOCK_FILENAME_TEMPLATE: str = '{}' + NAME_SEPARATOR + 'metadata.' + fu.CSV_FILE_EXTENSION
__STAT_FILENAME_TEMPLATE: str = '{}' + NAME_SEPARATOR + 'stats.' + fu.CSV_FILE_EXTENSION
__PREPROCESSING_FILENAME_TEMPLATE: str = '{}' + NAME_SEPARATOR + 'preprocessing.' + fu.PICKLE_FILE_EXTENSION


def compute_data_meta_data_file_path(str_id: str, parent_dir_path: str, *other_filename_prefixes: str)\
        -> Tuple[str, str]:
    data_file_template_path, metadata_file_template_path = \
        compute_data_meta_data_file_template_path(parent_dir_path, *other_filename_prefixes)
    return data_file_template_path.format(str_id), metadata_file_template_path.format(str_id)


def compute_data_meta_data_file_template_path(parent_dir_path: str, *other_filename_prefixes: str) -> Tuple[str, str]:
    data_filename_template = __create_path_prefix(parent_dir_path, *other_filename_prefixes,
                                                  __DATA_BLOCK_FILENAME_TEMPLATE)
    metadata_filename_template = __create_path_prefix(parent_dir_path, *other_filename_prefixes,
                                                      __METADATA_BLOCK_FILENAME_TEMPLATE)
    return data_filename_template, metadata_filename_template


def compute_stat_file_path(str_id: str, parent_dir_path: str, *other_filename_prefixes: str) -> str:
    stat_file_path_prefix = __create_path_prefix(parent_dir_path, *other_filename_prefixes, str_id)
    return __STAT_FILENAME_TEMPLATE.format(stat_file_path_prefix)


def compute_preprocessing_file_path(str_id: str, preprocessing_kind: str, parent_dir_path: str) -> str:
    preprocessing_file_path_prefix = __create_path_prefix(parent_dir_path, str_id, preprocessing_kind)
    return __PREPROCESSING_FILENAME_TEMPLATE.format(preprocessing_file_path_prefix)


def create_period_str(period: Period) -> str:
    return __parts_concatenation(period)


# Sort the labels alphabetically.
def sort_labels(label_ids: Iterable[LabelId]) -> Sequence[LabelId]:
    return sorted(label_ids)


def __parts_concatenation(parts: Iterable) -> str:
    return functools.reduce(lambda x, y: f'{x}{NAME_SEPARATOR}{y}', parts)


def __create_path_prefix(parent_dir_path: str, *identifiers) -> str:
    filename_prefix = __parts_concatenation(identifiers)
    return path.join(parent_dir_path, filename_prefix)


def __all_test():
    parent_dir_path = '/parent_dir_path'
    str_id = 'id'
    print(compute_stat_file_path(str_id, parent_dir_path, 'otherId'))
    print(compute_data_meta_data_file_path(str_id, parent_dir_path, 'otherId'))
    print(compute_data_meta_data_file_template_path(str_id, parent_dir_path, 'otherId'))
    print(compute_preprocessing_file_path(str_id, 'kind', parent_dir_path))


if __name__ == '__main__':
    __all_test()
