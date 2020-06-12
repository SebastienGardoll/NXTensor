#!/bin/bash

                              #### ENV VARS TO BE SET ####

# These environment variables must be set before running this script.

# CONDA_HOME is the path of the Anaconda or Miniconda home (not the env path ; i.e. /home/$USER/miniconda2)

# CONDA_ENV_NAME is the name of your virtual Python environment where is installed the required packages
# See README.md file.

# EXTRACTION_CONF_FILE_PATH is the path of the extraction configuration file.

                                  ##### SETTINGS #####
# Python:
export PYTHONUNBUFFERED='true'
ERROR_EXIT_CODE=1
set -e

                                  ##### FUNCTIONS #####

function source_conda_env
{
  source "${CONDA_HOME}/bin/activate" "${CONDA_HOME}/envs/${CONDA_ENV_NAME}"
}

function check_env_vars
{
  if [[ -z "${CONDA_HOME}" ]]; then
    echo "> [ERROR] CONDA_HOME env var must be set. Abort."
    exit ${ERROR_EXIT_CODE}
  fi

  if [[ -z "${CONDA_ENV_NAME}" ]]; then
    echo "> [ERROR] CONDA_ENV_NAME env var must be set. Abort."
    exit ${ERROR_EXIT_CODE}
  fi

  if [[ -z "${EXTRACTION_CONF_FILE_PATH}" ]]; then
    echo "> [ERROR] EXTRACTION_CONF_FILE_PATH env var must be set. Abort."
    exit ${ERROR_EXIT_CODE}
  fi
}

                                     ##### MAIN #####

check_env_vars

echo "> source conda env: ${CONDA_ENV_NAME}"
source_conda_env

if [[ -z "${SCRIPT_DIR_PATH}" ]]; then
  # We need absolute path.
  SCRIPT_DIR_PATH="$(dirname $0)" ; cd "${SCRIPT_DIR_PATH}" ; SCRIPT_DIR_PATH="$(pwd)"
  # Export the script directory path only one time because Torque copy this script.
  # So the path can't be computed when main.py submits this script as a Toque job.
  export SCRIPT_DIR_PATH
fi

echo "> starting main.py ($(date))"

python3 "${SCRIPT_DIR_PATH}/main.py"

echo "> main.py is completed with return code: ${?} ($(date))"

exit 0
