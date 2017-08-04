import setuptools

setuptools.setup(
    name='adlkit',
    version='0.3.1',
    author='Amir Farbin, William Hilliard, Ryan Reece',
    author_email='wghilliard@anomalousdl.com',
    url='https://github.com/anomalousdl/adlkit',
    install_requires=[
        'keras',
        'numpy',
        'h5py',
        'tensorflow'
    ],
    packages=setuptools.find_packages()
)
