FROM alpine:edge
RUN echo http://dl-cdn.alpinelinux.org/alpine/edge/testing >> \
     /etc/apk/repositories && \
     apk upgrade --update-cache --available

RUN apk add curl python gdbm py-gdbm tzdata linux-headers gcc python-dev \
     musl-dev py-numpy py-numpy-dev hdf5-dev cython-dev py-scipy && \
     cp /usr/share/zoneinfo/US/Central /etc/localtime && \
     curl -O https://bootstrap.pypa.io/get-pip.py && \
     python get-pip.py && \
     mkdir /data && \
     pip install h5py pytest && \
     mkdir /root/.keras

COPY ../keras.json /root/.keras/keras.json

COPY ./data/ /data/

#RUN echo fs.file-max = 32000 | tee -a /etc/sysctl.conf && \
#      echo root soft nofile 4096 | tee -a /etc/
#      echo root hard nofile 10240