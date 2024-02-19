#!/bin/bash

sudo apt-get update
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# monitoring module
sudo apt install -y glances

python3 -m pip install -r requirements.txt
