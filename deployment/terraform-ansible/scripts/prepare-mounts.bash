#!/bin/bash

sudo tuned-adm profile latency-performance &&
sudo /sbin/mkfs.xfs /dev/nvme0n1 &&
sudo /sbin/mkfs.xfs /dev/nvme1n1 &&
sudo mkdir -p /mnt/journal &&
sudo mkdir -p /mnt/storage &&
sudo mount -o defaults,noatime,nodiscard,nobarrier /dev/nvme0n1 /mnt/journal &&
sudo mount -o defaults,noatime,nodiscard,nobarrier /dev/nvme1n1 /mnt/storage