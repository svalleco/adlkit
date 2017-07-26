FROM fedora:latest

RUN dnf install python gcc hdf5-devel -y && \
    pip install numpy h5py pytest

COPY ./keras.json /root/.keras/keras.json

COPY ./data/ /data/

