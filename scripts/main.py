#!/usr/bin/env python3

import sys
from typing import Callable, Mapping, Sequence, Tuple

import os
import os.path as path

import subprocess

__EXTRACTION_CONF_FILE_PATH_VARNAME = 'EXTRACTION_CONF_FILE_PATH'
__FUNCTION_VARNAME = 'FUNCTION_NAME'
__VARIABLE_ID_VARNAME = 'VARIABLE_ID'
__SCRIPT_DIR_PATH_VARNAME = 'SCRIPT_DIR_PATH'

__CONDA_HOME_VARNAME = 'CONDA_HOME'
__CONDA_ENV_NAME_VARNAME = 'CONDA_ENV_NAME'

__QSUB_TIMEOUT = 30  # In seconds.

# Enable execution of this script from Torque and command line.
__SCRIPT_DIR_PATH = os.environ[__SCRIPT_DIR_PATH_VARNAME]
sys.path.append(path.join(__SCRIPT_DIR_PATH, '..'))

__ERROR_EXIT_CODE = 2


from nxtensor.extraction import ExtractionConfig
import nxtensor.variable_block_extraction as var_extract
import nxtensor.assembly as chan_assembly


# This function extract and assemble tensors.
def main(extraction_conf_file_path: str) -> None:
    var_extract.preprocess_extraction(extraction_conf_file_path)
    # We have to re-execute main.sh because main.py needs sourcing the Conda environment.
    # main.py will execute the extraction function accordingly to the __FUNCTION_VARNAME env var.
    script_file_path = path.join(__SCRIPT_DIR_PATH, 'main.sh')

    env_vars = dict()
    env_vars[__EXTRACTION_CONF_FILE_PATH_VARNAME] = extraction_conf_file_path
    env_vars[__FUNCTION_VARNAME] = 'extraction'
    env_vars[__CONDA_HOME_VARNAME] = os.environ[__CONDA_HOME_VARNAME]
    env_vars[__CONDA_ENV_NAME_VARNAME] = os.environ[__CONDA_ENV_NAME_VARNAME]
    env_vars[__SCRIPT_DIR_PATH_VARNAME] = __SCRIPT_DIR_PATH

    extraction_conf = ExtractionConfig.load(extraction_conf_file_path)
    variables_ids = list(extraction_conf.get_variables().keys())

    if extraction_conf.qsub_log_dir_path:
        os.makedirs(extraction_conf.qsub_log_dir_path, exist_ok=True)

    job_ids = list()
    for variable_id in variables_ids:
        job_name = f'{variable_id}_extraction'
        env_vars[__VARIABLE_ID_VARNAME] = variable_id

        args = create_torque_job(script_file_path=script_file_path,
                                 job_name=job_name,
                                 walltime=extraction_conf.max_walltime,
                                 mem=extraction_conf.extraction_mem_foot_print,
                                 node_specs=(1, extraction_conf.nb_process),
                                 env_vars=env_vars,
                                 log_dir_path=extraction_conf.qsub_log_dir_path)
        job_id = handle_torque_job(job_name, args)
        job_ids.append(job_id)
    # Create the assembly job that will be executed after the extraction jobs complete.
    env_vars[__FUNCTION_VARNAME] = 'assemble'
    job_name = f'{extraction_conf.str_id}_assembly'
    args = create_torque_job(script_file_path=script_file_path,
                             job_name=job_name,
                             walltime=extraction_conf.max_walltime,
                             mem=extraction_conf.assembly_mem_foot_print,
                             node_specs=(1, len(variables_ids)),
                             env_vars=env_vars,
                             job_dependencies=job_ids,
                             log_dir_path=extraction_conf.qsub_log_dir_path)
    handle_torque_job(job_name, args)


def extraction(extraction_conf_file_path: str) -> None:
    variable_id = os.environ[__VARIABLE_ID_VARNAME]
    var_extract.extract(extraction_conf_file_path, variable_id)


def assemble(extraction_conf_file_path: str) -> None:
    extraction_conf = ExtractionConfig.load(extraction_conf_file_path)
    nb_workers = len(extraction_conf.variable_file_paths)
    chan_assembly.preprocessing(extraction_conf_file_path)
    chan_assembly.channel_building_batch(extraction_conf_file_path, nb_workers)
    nb_workers = len(extraction_conf.tensor_dataset_ratios)
    chan_assembly.channel_stacking_batch(extraction_conf.str_id, extraction_conf_file_path, nb_workers)


def handle_torque_job(job_name: str, args: Sequence[any]) -> str:
    try:
        print(f"> submitting job {job_name}")
        job_id: str = submit_torque_job(args)
        print(f"> job {job_name} has been successfully submitted and gets the id: {job_id}")
        return job_id
    except Exception as e:
        msg = f'> [ERROR] something went wrong when submitting the job {job_name}: {str(e)}. Abort.'
        print(msg)
        sys.exit(__ERROR_EXIT_CODE)


def submit_torque_job(args: Sequence[any]) -> str:
    process = subprocess.run(args=args, capture_output=True, encoding='utf8', shell=False, timeout=__QSUB_TIMEOUT)
    if process.returncode != 0:
        raise Exception(process.stderr.strip())
    else:
        job_id: str = process.stdout.strip()
        return job_id


def create_torque_job(script_file_path: str,
                      job_name: str = None,
                      walltime: str = None,  # i.e. '01:59:59'
                      mem: str = None,  # i.e. 10gb
                      vmem: str = None,  # i.e. 10gb
                      # node_specs[0] is the number_nodes ; node_specs[1] is number of cores of per node.
                      node_specs: Tuple[int, int] = None,
                      env_vars: Mapping[str, str] = None,  # The environment variable name and their value.
                      job_dependencies: Sequence[str] = None,  # The ids of the job that this job depends to.
                      # The path of the directory where Torque will create the log of the job.
                      log_dir_path: str = None) -> Sequence[any]:
    result = list()
    result.append('qsub')
    result.append('-j')
    result.append('oe')
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

    if job_dependencies:
        formatted_ids = ':'.join(job_dependencies)
        result.append(f"-W depend=afterok:{formatted_ids}")

    if walltime:
        result.append('-l')
        result.append(f'walltime={walltime}')

    if mem:
        result.append('-l')
        result.append(f'mem={mem}')

    if vmem:
        result.append('-l')
        result.append(f'vmem={vmem}')
    elif mem:
        result.append('-l')
        result.append(f'vmem={mem}')

    if node_specs:
        number_nodes = node_specs[0]
        number_cores_per_node = node_specs[1]
        result.append('-l')
        result.append(f"nodes={number_nodes}:ppn={number_cores_per_node}")

    result.append(script_file_path)
    return result


__FUNCTIONS: Mapping[str, Callable[[str], None]] = {'main': main, 'extraction': extraction, 'assemble': assemble}


if __name__ == '__main__':
    extraction_conf_path = os.environ[__EXTRACTION_CONF_FILE_PATH_VARNAME]

    if __FUNCTION_VARNAME in os.environ:
        function_name = os.environ[__FUNCTION_VARNAME]
    else:
        function_name = 'main'
    __FUNCTIONS[function_name](extraction_conf_path)
    sys.exit(0)
