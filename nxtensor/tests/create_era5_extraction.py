from os import path

from nxtensor.extraction import ClassificationLabel, ExtractionConfig, ExtractionShape
from nxtensor.utils.time_resolutions import TimeResolution
from nxtensor.utils.db_types import DBType
from nxtensor.variable import Variable

import nxtensor.utils.csv_utils as cu


def bootstrap_cyclone_labels(label_parent_dir: str) -> None:
    dataset_ids = ['2ka', '2kb', '2000', '2000_10', 'all']
    data_parent_dir = '/data/sgardoll/cyclone_data/dataset'
    filename_postfix = 'dataset.csv'
    db_filename_template = '{dataset_id}_{label_id}_{filename_postfix}'
    db_format = DBType.CSV
    db_time_resolution = TimeResolution.HOUR
    db_format_options = cu.create_csv_options(separator=',', header=0, line_terminator='\\n', encoding='utf8',
                                              quote_char='"')

    db_meta_data_mapping = dict(lat='lat', lon='lon', year='year', month='month', day='day', hour='hour')

    def create_label(dataset_id: str, num_id: float, label_id: str) -> None:
        label = ClassificationLabel(label_id, dataset_id)
        label.num_id = num_id
        label.db_file_path = ''
        label.db_format = db_format
        label.db_format_options = db_format_options
        label.db_meta_data_mapping = db_meta_data_mapping
        label.db_time_resolution = db_time_resolution
        db_filename = db_filename_template.format(dataset_id=dataset_id,
                                                  label_id=label_id,
                                                  filename_postfix=filename_postfix)
        label.db_file_path = path.join(data_parent_dir, db_filename)
        label_file_path = path.join(label_parent_dir, label.compute_filename())
        label.save(label_file_path)

    for dataset_id in dataset_ids:
        create_label(dataset_id, 1.0, 'cyclone')
        create_label(dataset_id, 0.0, 'no_cyclone')


def bootstrap_cyclone_extraction_configs(config_parent_dir: str) -> None:
    dataset_ids = ['2ka', '2kb', '2000', '2000_10', 'all']
    era5_variables = ['msl', 'tcwv', 'u10', 'v10', 'ta200', 'ta500', 'u850', 'v850', 'wsl10', 'dummy']
    dask_scheduler = 'single-threaded'
    x_size = 32
    y_size = 32
    extraction_shape = ExtractionShape.SQUARE
    nb_process = 4

    variable_file_paths = list()
    for var_str_id in era5_variables:
        var_filename = Variable.generate_filename(var_str_id)
        variable_file_paths.append(path.join(config_parent_dir, var_filename))

    for dataset_id in dataset_ids:
        labels = [f"{config_parent_dir}/{ClassificationLabel.generate_filename(dataset_id, 'cyclone')}",
                  f"{config_parent_dir}/{ClassificationLabel.generate_filename(dataset_id, 'no_cyclone')}"]

        output_parent_dir = path.join(config_parent_dir, f'{dataset_id}_extraction')
        extract_config = ExtractionConfig(dataset_id)
        extract_config.dask_scheduler = dask_scheduler
        extract_config.x_size = x_size
        extract_config.y_size = y_size
        extract_config.variable_file_paths = variable_file_paths
        extract_config.label_file_paths = labels
        extract_config.extraction_shape = extraction_shape
        extract_config.blocks_dir_path = path.join(output_parent_dir, 'blocks')
        extract_config.tmp_dir_path = path.join(output_parent_dir, 'tmp')
        extract_config.nb_process = nb_process

        file_path = path.join(config_parent_dir, ExtractionConfig.generate_filename(dataset_id))
        extract_config.save(file_path)


def test_load(config_parent_dir_path: str) -> None:
    dataset = ['2ka', '2kb', '2000', '2000_10', 'all']
    for str_id in dataset:
        filename = ExtractionConfig.generate_filename(str_id)
        conf = ExtractionConfig.load(path.join(config_parent_dir_path, filename))
        conf.get_variables()
        conf.get_labels()


if __name__ == '__main__':
    config_files_parent_dir_path = '/home/sgardoll/extraction_config'

    print("> creating the labels")
    bootstrap_cyclone_labels(config_files_parent_dir_path)

    print("> creating the extraction configuration files")
    bootstrap_cyclone_extraction_configs(config_files_parent_dir_path)

    print("> loading all created files")
    test_load(config_files_parent_dir_path)
