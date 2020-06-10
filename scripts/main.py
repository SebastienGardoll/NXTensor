#!/usr/bin/env python3

import sys
from typing import Callable, Mapping, Sequence, Tuple

import os
import subprocess

# Enable execution from the parent directory.
sys.path.append('../')

from nxtensor.extraction import ExtractionConfig
import nxtensor.variable_block_extraction as var_extract


__EXTRACTION_CONF_FILE_PATH_VARNAME = 'EXTRACTION_CONF_FILE_PATH'
__FUNCTION_VARNAME = 'FUNCTION_NAME'
__VARIABLE_ID_VARNAME = 'VARIABLE_ID'


# This function extract and assemble tensors.
def main(extraction_conf_file_path: str) -> None:
    var_extract.preprocess_extraction(extraction_conf_file_path)

    env_vars = dict()
    env_vars[__EXTRACTION_CONF_FILE_PATH_VARNAME] = extraction_conf_file_path
    env_vars[__FUNCTION_VARNAME] = 'extraction'

    extraction_conf = ExtractionConfig.load(extraction_conf_file_path)
    variables_ids = [extraction_conf.get_variables().key]

    script_file_path = __file__

    for variable_id in variables_ids:
        job_name = f'{variable_id}_extraction'
        env_vars[__VARIABLE_ID_VARNAME] = variable_id

        args = forge_qsub_cmd(script_file_path=script_file_path,
                              walltime=extraction_conf.max_walltime,
                              mem=extraction_conf.max_mem_foot_print,
                              node_specs=(1, extraction_conf.nb_process),
                              env_vars=env_vars,
                              job_name=job_name,
                              log_dir_path=extraction_conf.qsub_log_dir_path)

        process = subprocess.run(args=args, capture_output=True)
        print(process.stdout)


def forge_qsub_cmd(script_file_path: str,
                   walltime: str = None,  # i.e. '01:59:59'
                   mem: str = None,  # i.e. 10gb
                   vmem: str = None,  # i.e. 10gb
                   node_specs: Tuple[int, int] = None,
                   env_vars: Mapping[str, str] = None,
                   job_name: str = None,
                   log_dir_path: str = None) -> Sequence[any]:
    result = list()
    result.append('qsub')
    result.append('-j oe')
    if job_name:
        result.append('-N')
        result.append(job_name)

    if log_dir_path:
        result.append('-o')
        result.append(log_dir_path)

    if env_vars:
        result.append('-v')
        env_vars_opt_tmp = list()
        for key, value in env_vars.items():
            env_vars_opt_tmp.append(f'{key}={value}')
        result.append(f"{','.join(env_vars_opt_tmp)}")
        del env_vars_opt_tmp

    if walltime:
        result.append('-l')
        result.append(f'walltime={walltime}')

    if mem:
        result.append('-l')
        result.append(f'mem={mem}')

    if vmem:
        result.append('-l')
        result.append(f'vmem={vmem}')

    if node_specs:
        number_nodes = node_specs[0]
        number_cores_per_node = node_specs[1]
        result.append('-l')
        result.append(f"nodes={number_nodes}:ppn={number_cores_per_node}")

    result.append(script_file_path)
    return result


def extraction(extraction_conf_file_path: str) -> None:
    variable_id = os.environ[__VARIABLE_ID_VARNAME]
    var_extract.extract(extraction_conf_file_path, variable_id)


__FUNCTIONS: Mapping[str, Callable[[str], None]] = {'main': main, 'extraction': extraction}


if __name__ == '__main__':
    extraction_conf_path = os.environ[__EXTRACTION_CONF_FILE_PATH_VARNAME]
    function_name = os.environ[__FUNCTION_VARNAME]
    __FUNCTIONS[function_name](extraction_conf_path)
    sys.exit(0)
