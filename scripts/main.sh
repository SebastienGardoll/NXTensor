#!/bin/bash

                              #### ENV VARS TO BE SET ####

# These environment variables must be set before running this script.

# CONDA_HOME is the path of the Anaconda or Miniconda home (not the env path ; i.e. /home/$USER/miniconda2)

# CONDA_ENV_NAME is the name of your virtual Python environment where is installed the required packages
# See README.md file.

# EXTRACTION_CONF_FILE_PATH is the path of the extraction configuration file.

                                  ##### SETTINGS #####

readonly BASE_DIR_PATH="$(pwd)"
SCRIPT_DIR_PATH="$(dirname $0)"; cd "${SCRIPT_DIR_PATH}"
readonly SCRIPT_DIR_PATH="$(pwd)" ; cd "${BASE_DIR_PATH}"

# Python:
export PYTHONUNBUFFERED='true'

set -e

                                  ##### FUNCTIONS #####

function source_conda_env
{
  source "${CONDA_HOME}/bin/activate" ""${CONDA_HOME}/envs/${CONDA_ENV_NAME}""
}

function check_env_vars
{
  if [[ -z "${CONDA_HOME}" ]]; then
    echo "> [ERROR] CONDA_HOME env var must be set. Abort."
    exit 1
  fi

  if [[ -z "${CONDA_ENV_NAME}" ]]; then
    echo "> [ERROR] CONDA_ENV_NAME env var must be set. Abort."
    exit 1
  fi

  if [[ -z "${EXTRACTION_CONF_FILE_PATH}" ]]; then
    echo "> [ERROR] EXTRACTION_CONF_FILE_PATH env var must be set. Abort."
    exit 1
  fi
}

                                     ##### MAIN #####

check_env_vars

echo "> source conda env: ${CONDA_ENV_NAME}"
source_conda_env

cd "${SCRIPT_DIR_PATH}"

echo "> starting main.py ($(date))"
export FUNCTION_NAME='main'
python3 main.py

echo "> main.py is completed ($(date))"

exit 0
