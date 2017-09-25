import setuptools

from adlkit import __version__

setuptools.setup(
        name="adlkit",
        version=__version__,
        author="Amir Farbin, William Hilliard, Ryan Reece",
        author_email="wghilliard@anomalousdl.com",
        url="https://github.com/anomalousdl/adlkit",
        install_requires=[
            "keras",
            "numpy",
            "h5py",
            "theano",
            "billiard"
        ],
        packages=setuptools.find_packages()
)
