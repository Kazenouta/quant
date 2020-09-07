## 托管者安装运行
- 下载
wget https://www.fmz.com/dist/robot_linux_amd64.tar.gz
- 解压
tar -xzvf robot_linux_amd64.tar.gz
- 测试运行
./robot -s node.fmz.com/47530784 -p b13781131889
- 后台运行
nohup ./robot -s node.fmz.com/47530784 -p b13781131889 &