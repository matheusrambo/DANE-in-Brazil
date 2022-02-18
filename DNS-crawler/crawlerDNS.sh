#!/bin/bash
crawler-dns(){

readonly Terminal2="Terminal2.sh" # O script a ser executado
chmod +x $Terminal2
gnome-terminal -x bash -c "./$Terminal2; exec $SHELL"

cd ~/
cd dns-crawler 
source .venv/bin/activate 
dns-crawler-controller domain-list2.txt > results.json

cd ~/
cd dns-crawler
cp results.json /home/rambotcc/TCC/DNS-to-SMTP/entrada/`date "+%F"`-dns.json
cd ~/
cd TCC
cd DNS-to-SMTP

python3 DNS-to-SMTP.py
python3 DNS-to-TLSA.py

cp input.txt /home/rambotcc/go/tlsa-scan/input-tlsa.txt
mkdir /home/rambotcc/TCC/DNS-crawler/TLSA/`date "+%F"`
mkdir /home/rambotcc/TCC/DNS-crawler/TLSA/`date "+%F"`/16
cd .. 
cd SMTP-crawler
cd saida
mkdir `date "+%F"`
cd `date "+%F"`
mkdir 16
cd ..
cd ..
source /etc/profile

./starttls-scan saopaulo 1 /home/rambotcc/TCC/DNS-to-SMTP/saida/`date "+%F"`-dns.txt /home/rambotcc/TCC/SMTP-crawler/saida/`date "+%F"`/16/certs_0.txt

cd ~/
cd go
cd tlsa-scan

./tlsa-scan saopaulo 1 input-tlsa.txt /home/rambotcc/TCC/DNS-crawler/TLSA/`date "+%F"`/16/tlsa_0.txt
}
crawler-dns
