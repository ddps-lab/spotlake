sudo DEBIAN_FRONTEND=noninteractive apt-get dist-upgrade -y

sudo apt-get update
sudo apt-get install awscli -y
sudo apt-get install python3-pip -y
aws --version
pip --version

pip install -r requirements.txt
pip install --upgrade awscli

aws configure
