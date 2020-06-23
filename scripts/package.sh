#!/bin/bash

set -e

# Requires wheel setuptools twine
# conda install wheel setuptools twine

# Regenerate distribution packages
python setup.py sdist bdist_wheel

python -m twine upload dist/*

rm -fr build dist NXTensor.egg-info
