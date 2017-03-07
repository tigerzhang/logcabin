#! /bin/bash

BUILD_DIR=build
CPU_CORE=`nproc`

if [ ! -d "${BUILD_DIR}" ]; then
    mkdir ${BUILD_DIR}
fi

git submodule update --init
cd ${BUILD_DIR}
cmake ..
make -j${CPU_CORE}
