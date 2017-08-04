**Please read the `LICENSE.txt` before use.**

# ADLKit

[![Join the chat at https://gitter.im/anomalousdl/adlkit](https://badges.gitter.im/anomalousdl/adlkit.svg)](https://gitter.im/anomalousdl/adlkit?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

### What is ADLKit?
*A library that makes deep learning easy...*

Often it is difficult to wire up all the things necessary to start
training with Deep Neural Networks on large datasets. In order to
combat this barrier to entry, libraries such as Keras and TensorFlow,
Theano, Scikit-Learn, etc., are used to do the heavy lifting for you.
However, it seems that these libraries all leave out important pieces
that really unlock the potential of your hardware. There is a demand
for a framework that combines all of these great tools together to
make it easier than ever to dive into scalable Deep Learning without
the hassle of looking under the hood.

### Installation:

_Tested on Python 2.7.13_

###### Assumptions
- `Python` == 2.7.X
- `pip` >= [9.0.X](https://pip.pypa.io/en/stable/installing/)
- `git`

###### Procedure
1. Clone this repository:
    ```bash
    git clone https://github.com/anomalousdl/adlkit
    ```

3. (Optional) Setup a python virtual environment:
   ```bash
   pip install virtualenvwrapper
   source /usr/local/bin/virtualenvwrapper.sh
   mkvirtualenv workspace
   workon workspace
   ```

3. Use pip to install the package .
    ```bash
    pip install -e ./adlkit
    ```

### More Info
  - `adlkit/data_provider/README.md`
  - `adlkit/data_catalog/README.md`
