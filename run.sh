#!/usr/bin/env bash

Xvfb :1 -screen 0 1024x768x24 &> xvfb.log &
echo "Xvbf is running..."

./hec-hms-421/hec-hms.sh &
echo "hec-hms is running..."

cd /home/hec-dssvue201
./hec-dssvue.sh &
#echo "hec-dssvue is running..."
cd /home
/bin/bash
