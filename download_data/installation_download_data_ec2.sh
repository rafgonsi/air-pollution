#!/bin/bash
set -e  # fail in case of any error

sudo timedatectl set-timezone Europe/Warsaw

wget https://repo.anaconda.com/archive/Anaconda3-2019.07-Linux-x86_64.sh -O Anaconda_installer.sh
chmod +x Anaconda_installer.sh
./Anaconda_installer.sh -b -p ~/Anaconda3
rm Anaconda_installer.sh

eval "$(/home/ubuntu/Anaconda3/bin/conda shell.bash hook)"

conda create -n air-download-data python=3.6.9 -y
conda activate air-download-data
conda install -c anaconda pandas=0.25.1 requests=2.22.0 retrying=1.3.3 -y
conda install -c conda-forge boto3=1.9.243 -y

echo '@reboot /home/ubuntu/air-pollution/download_data/startup.sh' | crontab -
mkdir home/ubuntu/logs
touch home/ubuntu/logs/last_run.log
