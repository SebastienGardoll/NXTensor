import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="NXTensor",
    version="0.3.3",
    author="Sébastien Gardoll",
    author_email="sebastien@gardoll.fr",
    description="NXTensor is a tensor making framework based on Xarray. It automates the extraction " +
                "of multichannel images (tensors) from NetCDF time series of geolocated data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: CEA CNRS Inria Logiciel Libre License, version 2.1 (CeCILL-2.1)",
        "Development Status :: 2 - Pre-Alpha",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Scientific/Engineering :: Image Recognition",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
