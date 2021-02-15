#!/usr/bin/env bash
set -ex

g++ -Wall -Werror -g -o target/export_ioctl_constants export_ioctl_constants.cpp
./target/export_ioctl_constants
