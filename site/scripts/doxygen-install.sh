#!/bin/bash

(
  cd scripts
  git clone https://github.com/doxygen/doxygen.git
  cd doxygen && mkdir build && cd build
  cmake -G "Unix Makefiles" ..
  make
  make install
)
