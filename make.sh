#!/usr/bin/env bash
git submodule add https://github.com/facebook/rocksdb rocksdb
git submodule update --init
cd rocksdb/
make static_lib
sudo make install
cd -
deps=("lz4" "jemalloc" "crypto++" "protobuf" "snappy" "bz2")
for i in "${deps[@]}" ; do
	sudo apt-get install lib$i-dev
done
sudo apt-get install protobuf-compiler
