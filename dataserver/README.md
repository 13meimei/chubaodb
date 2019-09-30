Chubao DataServer
-----------------------------------

# Prerequisite
## Centos
yum install libcurl libcurl-devel

## Ubuntu
sudo apt install libcurl4 libcurl4-openssl-dev

# build
```sh
mkdir build
cd build
cmake ..
make -j `nproc`
````
