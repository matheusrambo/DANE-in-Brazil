#!/bin/bash
dns-crawler(){
cd ~/
cd dns-crawler
source .venv/bin/activate

sleep 10

dns-crawler-workers 12
}
dns-crawler
