curl -s https://packagecloud.io/install/repositories/tarantool/1_10/script.rpm.sh | sudo bash
sudo yum -y install python-devel python-pip
sudo pip install tarantool msgpack
