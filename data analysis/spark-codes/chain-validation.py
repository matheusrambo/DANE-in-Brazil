#!/usr/bin/python3
from pyspark import SparkContext, StorageLevel

import sys, os
import dns as mydns
import dns.message as mymessage
import base64
from OpenSSL import crypto
import hashlib
import json
import re
import subprocess
import struct
import time
import random
import shutil
import datetime

root_cert_path = "/etc/ssl/certs/ca-certificates.crt" # path to root-ca-list, ex) /home/ubuntu/root-ca-list
dependency_path = "/home/rambotcc/TCC/análise-dados/spark-codes/dependencies.zip" # path to 'dependencies.zip'
input_path = "/home/rambotcc/TCC/raw-merge/merged_output/saopaulo" # ex) /user/hadoop/data/virginia
output_path = "/home/rambotcc/TCC/análise-dados/spark-codes/chain_output_saopaulo" # ex) /user/hadoop/chain_output_virginia
cert_path = "/run/shm/rambo" # ex) /run/shm/john, Temporary path for chain validation. Temporary certificates are generated in this directory. ***Caution: It may consume huge disk space


make_name_size = 2**16 - 1
root_certs = ''

def make_name():
    global make_name_size
    return struct.pack(">dHH", time.time(), random.randint(0, make_name_size), os.getpid() % 65536).hex()

def tojson(line):
    return json.loads(line)

def toCSV(data):
    return " ".join(str(d) for d in data)

def parseTLSA(raw):
    msg = base64.b64decode(raw)
    msg = mymessage.from_wire(msg)
    rrsetList = []

    for answer in msg.answer:
        if answer.rdtype == 52:
            rrsetList = rrsetList + [data.to_text() for data in answer]
    return rrsetList

def getCerts(data):
    from OpenSSL import crypto

    def getCertRecord(certs):
        periodList = []
        for crt in certs:
            pem = base64.b64decode(crt)
            cert = crypto.load_certificate(crypto.FILETYPE_PEM, pem)
            notBefore = datetime.datetime.strptime(cert.get_notBefore().decode(), "%Y%m%d%H%M%SZ")
            notAfter = datetime.datetime.strptime(cert.get_notAfter().decode(), "%Y%m%d%H%M%SZ")
            
            periodList.append((notBefore, notAfter))

        return periodList

    if not 'certs' in data['starttls']:
        return {"name": "None"}

    certs = data['starttls']['certs']
    periodList = getCertRecord(certs)

    result = {"name":" ".join(certs), "certs":certs, "periods":periodList}

    return result


def chainValidRaw(usage, time, certs):
    from OpenSSL import crypto
    
    global cert_path, root_certs
    '''
    #Return
    False (Chain is valid)
    True (Chain is not valid)
    '''

    certNum = len(certs)
    leaf = base64.b64decode(certs[0]).decode()

    while True:
        tmp_folder = "ssl_verify_" + make_name()
        base_path = os.path.join(cert_path, tmp_folder)
        if not os.path.isdir(base_path):
            os.makedirs(base_path)
            break

    leaf_filename = os.path.join(base_path, "leaf.pem")
    f_leaf = open(leaf_filename, "w")
    f_leaf.write(leaf)
    f_leaf.close()

    root_filename = os.path.join(base_path, "root.pem")
    f_root = open(root_filename, "w")
    f_root.write(root_certs)
    f_root.close()

    eTime = datetime.datetime.strptime(time, "%Y%m%d %H:%M:%S")
    eTime = str(int(eTime.timestamp()))

    if certNum == 1:
        if usage == "0" or usage == "1":
            query = ['openssl', 'verify', '-attime', eTime, '-CAfile', root_filename, leaf_filename]
        else:
            shutil.rmtree(base_path)
            return False, ""
    else:
        inter = ""
        if usage == "0" or usage == "1":
            for cert in certs[1:]:
                inter = inter + base64.b64decode(cert).decode() + "\n"
 
            inter_filename = os.path.join(base_path, "inter.pem")
            f_inter = open(inter_filename, "w")
            f_inter.write(inter)
            f_inter.close()

            query = ['openssl', 'verify', '-attime', eTime, '-CAfile', root_filename, '-untrusted', inter_filename, leaf_filename]
        
        else:
            root = base64.b64decode(certs[-1]).decode()
            root_filename = os.path.join(base_path, "root.pem")
            f_root = open(root_filename, "w")
            f_root.write(root)
            f_root.close()

            for cert in certs[1:-1]:
                inter = inter + base64.b64decode(cert).decode() + "\n"
        
            if inter == "":
                query = ['openssl', 'verify', '-attime', eTime, '-CAfile', root_filename, leaf_filename]
            else:
                inter_filename = os.path.join(base_path, "inter.pem")
                f_inter = open(inter_filename, "w")
                f_inter.write(inter)
                f_inter.close()

                query = ['openssl', 'verify', '-attime', eTime, '-CAfile', root_filename, '-untrusted', inter_filename, leaf_filename]
        
 
    process = subprocess.Popen(query, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdoutdata, stderrdata = process.communicate()
    result = stdoutdata.decode()

    shutil.rmtree(base_path)
    fails = ["error", "fail", "Error", "Expire"]
    for fail in fails:
        if fail in result:
            return False, result
    
    return True, stderrdata.decode()

def chainValidSet(data):
    if data[0] == "None":
        return "None"

    rawData = [t for t in data[1]]
    periodList = rawData[0]['periods']
    certList = rawData[0]['certs']

    time = periodList[0][0] + datetime.timedelta(hours=1)
    time = time.strftime("%Y%m%d %H:%M:%S")

    isValid0, error0 = chainValidRaw('0', time, certList)
    isValid2, error2 = chainValidRaw('2', time, certList)


    hashList = []
    for crt in certList:
        pem = base64.b64decode(crt)
        hashed = hashlib.sha256(pem).hexdigest()
        hashList.append(hashed)
    
    result = ""
    for hashed, period in zip(hashList, periodList):
        result = result + hashed + " " + period[0].strftime("%Y%m%d-%H") + " " + period[1].strftime("%Y%m%d-%H") + " "

    result = result + str(isValid0) + " " + str(isValid2)
    return result


def getStats(sc):
    global root_certs

    def chainGroup(x):
        return x["name"]

    f = open(root_cert_path, "r")
    root_certs = f.read()
    f.close()

    k = sc.textFile(os.path.join(input_path, "*"))\
            .map(tojson)\
            .map(getCerts)\
            .groupBy(chainGroup)\
            .map(chainValidSet)
    
    k.saveAsTextFile(output_path)


if __name__ == "__main__":
    sc = SparkContext(appName="DANE-RESEARCH-CHAIN-VALIDATION")
    sc.addPyFile(dependency_path)
    getStats(sc)
    sc.stop()

