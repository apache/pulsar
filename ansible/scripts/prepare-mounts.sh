#!/bin/bash

sudo tuned-adm profile latency-performance &&
mkfs.xfs /dev/nvme0n1 &&
mkfs.xfs /dev/nvme1n1 &&
mkdir -p /mnt/journal &&
mkdir -p /mnt/storage &&
mount -o defaults,noatime,nodiscard,nobarrier /dev/nvme0n1 /mnt/journal &&
mount -o defaults,noatime,nodiscard,nobarrier /dev/nvme1n1 /mnt/storage