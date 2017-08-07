#!/bin/bash

PLATFORM=$1
HTMLTEST_VERSION=0.4.1

(
    cd ~/bin
    wget https://github.com/wjdp/htmltest/releases/download/v${HTMLTEST_VERSION}/htmltest-${PLATFORM}
    chmod +x htmltest-${PLATFORM}
)