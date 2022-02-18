#!/user/bin/python3
from pyspark import SparkContext

import os
import sys
import dns as mydns
import dns.message as mymessage
import base64
from OpenSSL import crypto
import json
import datetime
import hashlib


seed_path = "" # path to seed file, ex) /home/ubuntu/seed-files/
dependency_path = "" # path to 'dependencies.zip'
input_path = "" # ex) /user/hadoop/data/virginia
output_path = "" # ex) /user/hadoop/rollover_cand_output_virginia


startTime = "2019-07-10" # Start date of our measurement
endTime = "2019-10-30" # Last date of our measurement

targetDnSet = set([])

def tojson(line):
    return json.loads(line)

def toCSV(line):
    return " ".join(str(d) for d in line)

def toTime(x):
    return datetime.datetime.strptime(x, '%Y%m%d %H')


def isLetsEncrypt(x):
    from OpenSSL import crypto

    for item in x:
        if not 'certs' in item['starttls']:
            continue
        pem = base64.b64decode(item['starttls']['certs'][0])
        cert = crypto.load_certificate(crypto.FILETYPE_PEM, pem)
        issuer = cert.get_issuer()
        if issuer.CN == None:
            continue
        if "Let's Encrypt" in issuer.CN:
            return True
    return False

def parseTLSA(encoded):
    msg = base64.b64decode(encoded)
    msg = mymessage.from_wire(msg)
    rrset = []

    for answer in msg.answer:
        if answer.rdtype == 52:
            rrset = rrset + [data.to_text() for data in answer]
    return set(rrset)

def getTLSA(d):
    if 'tlsa' in d:
        return parseTLSA(d['tlsa']['record_raw'])
    return None
    

def getCert(d):
    if 'certs' in d['starttls']:
        return set(d['starttls']['certs'])
    return None

def getKey(d):
    from OpenSSL import crypto
    certs = getCert(d)
    if certs == None:
        return None

    keys = set([])
    for cert in certs:
        crt = base64.b64decode(cert)
        crt = crypto.load_certificate(crypto.FILETYPE_PEM, crt)
        key = crt.get_pubkey()
        key = crypto.dump_publickey(crypto.FILETYPE_ASN1, key)
        key = base64.b64encode(key)
        keys.add(key)
    return keys

def rolloverPrepare(d):
    dn = d[0]
    dataList = d[1]
    if dataList == None:
        return (dn, "NotExistWholePeriod")

    tlsaChanged = 0
    certChanged = 0
    keyChanged = 0

    prevKey = None
    prevCert = None
    prevTlsa = None
    for data in dataList:
        currTlsa = getTLSA(data)
        currCert = getCert(data)
        currKey = getKey(data)

        if not prevTlsa == currTlsa:
            if prevTlsa == None:
                prevTlsa = getTLSA(data)
            else:
                if not currTlsa == None:
                    tlsaChanged += 1
                    prevTlsa = currTlsa
        if not prevCert == currCert:
            if prevCert == None:
                prevCert = getCert(data)
            else:
                if not currCert == None:
                    certChanged += 1
                    prevCert = currCert
        if not prevKey == currKey:
            if prevKey == None:
                prevKey = getKey(data)
            else:
                if not currKey == None:
                    keyChanged += 1
                    prevKey = currKey
    return (dn, tlsaChanged, certChanged, keyChanged)
    

def genList(data):
    
    if data[0] in targetDnSet:

        dataList = [t for t in data[1]]
        dataList = sorted(dataList, key=lambda e: toTime(e['time']))
        return (data[0], dataList)
    else:
        return (data[0], None)


def genTargetDn():
    global targetDnSet

    startSet = set([])
    f = open(os.path.join(seed_path, "tlsanames-" + startTime + ".txt"), "r")
    while True:
        line = f.readline()
        if not line: break
        line = line[:-1]

        line = line.split(".")
        port = line[0].replace("_", "")
        dn = ".".join(line[2:])

        startSet.add(dn + ":" + port)
    f.close()

    endSet = set([])
    f = open(os.path.join(seed_path, "tlsanames-" + endTime + ".txt"), "r")
    while True:
        line = f.readline()
        if not line: break
        line = line[:-1]

        line = line.split(".")
        port = line[0].replace("_", "")
        dn = ".".join(line[2:])

        endSet.add(dn + ":" + port)
    f.close()

    targetDnSet = startSet.intersection(endSet)

def getStats(sc):
    def dnGroup(x):
        return x['domain'] + ":" + x['port']


    genTargetDn()
    k = sc.textFile(input_path)\
            .map(tojson)\
            .groupBy(dnGroup)\
            .map(genList)\
            .map(rolloverPrepare)\
            .map(toCSV)

    k.saveAsTextFile(output_path)

if __name__ == "__main__":
    sc = SparkContext(appName="DANE-ROLLOVER-CANDIDATE")
    sc.addPyFile(dependency_path)
    getStats(sc)
    sc.stop()
