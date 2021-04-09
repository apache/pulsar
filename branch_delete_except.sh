#!/bin/bash

for branch in `git branch`; do
    if [ "$branch" != "master" ] && [ "$branch" != "penghui/broker_batch" ]; then
        git branch -D "$branch"
    fi
done
