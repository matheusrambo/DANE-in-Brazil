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


chain_path = "/home/rambotcc/TCC/análise-dados/spark-codes/chain_output_saopaulo" # path to chain validation result (output of chain-validation.py), ex) /home/ubuntu/results/chain_output_virginia/
dependency_path = "/home/rambotcc/TCC/análise-dados/spark-codes/dependencies.zip" # path to 'dependencies.zip'
input_path = "/home/rambotcc/TCC/raw-merge/merged_output/saopaulo" # ex) /user/hadoop/data/virginia/
output_path = "/home/rambotcc/TCC/análise-dados/spark-codes/dane_output_saopaulo" # ex) /user/ubuntu/dane_output_virginia/


cert_path = "/etc/ssl/certs/" # Path of root CA certificates in Ubuntu, if you use other OS you have to set this path manually 
make_name_size = 2**16 - 1 
root_certs = {}
chainMap = {}

def make_name():
    global make_name_size
    return struct.pack(">dHH", time.time(), random.randint(0, make_name_size), os.getpid()).hex()

def tojson(line):
    return json.loads(line)

def toCSV(data):
    return " ".join(str(d) for d in data)

def tlsaCrawled(d):
    return int ('tlsa' in d)

def dnssec(d):
    '''    
    #Return
    -1: None (TLSA is not crawled)
    0: Secure
    1: Insecure
    2: Bogus
    3: Error value
    -1000: Wrong value
    '''

    if 'tlsa' in d:
        dnssec = d['tlsa']['dnssec']
        if dnssec == "Secure":
            return 0
        elif dnssec == "Insecure":
            return 1
        elif dnssec == "Bogus":
            return 2
        else:
            return -1000
    return -1

def starttlsCrawled(d):
    return int ('certs' in d['starttls'])

def getStarttlsErr(d):
    '''
    #Return
    -2: NoData (No log file)
    -1: None (STARTTLS certificate is cralwed)
    0: Error Code 4
    1: Error Code 5
    2: Unknown
    3: Connection related Error
    '''

    if 'why_fail' in d['starttls']:
        unknown = "EOF"
        connErrors = ["tls:", "read", "dial", "write", "remote", "short", "250"]
        
        error = d['starttls']['why_fail'].split()[0]
        if error[0] == "4":
            return 0
        elif error[0] == "5":
            return 1
        elif error == unknown:
            return 2
        elif error == "NoData":
            return -2
        else:
            return 3
    return -1

def parseTLSA(raw):
    msg = base64.b64decode(raw)
    msg = mymessage.from_wire(msg)
    rrsetList = []

    for answer in msg.answer:
        if answer.rdtype == 52:
            rrsetList = rrsetList + [data.to_text() for data in answer]
    return rrsetList

def matchCrt(matching, data, crt):
    from OpenSSL import crypto
    
    if matching == '0':
        crt = crypto.load_certificate(crypto.FILETYPE_ASN1, crt)
        hashed = crypto.dump_certificate(crypto.FILETYPE_PEM, crt).lower().decode()
        hashed = hashed.replace('-----begin certificate-----', '').replace('-----end certificate-----', '').replace('\n', '').lower()
        data = data.replace('-----begin certificate-----', '').replace('-----end certificate-----', '').replace('\n', '').lower()
    elif matching == '1':
        data = data.lower()
        hashed = hashlib.sha256(crt).hexdigest()
    elif matching == '2':
        data = data.lower()
        hashed = hashlib.sha512(crt).hexdigest()
    else:
        return False, "Matching-"+matching

    if hashed == data:
        return True, None
    return False, "CrtNotMatch"

def matchKey(matching, data, key):
    from OpenSSL import crypto
    if matching == '0':
        hashed = crypto.load_publickey(crypto.FILETYPE_ASN1, key)
        hashed = crypto.dump_publickey(crypto.FILETYPE_PEM, hashed).lower().decode()
        hashed = hashed.replace('-----begin public key-----', '').replace('-----end public key-----', '').replace('\n', '').lower()
        data = data.replace('-----begin public key-----', '').replace('-----end public key-----', '').replace('\n', '').lower()
    elif matching == '1':
        data = data.lower()
        hashed = hashlib.sha256(key).hexdigest()
    elif matching == '2':
        data = data.lower()
        hashed = hashlib.sha512(key).hexdigest()
    else:
        return False, "Matching-"+matching

    if hashed == data:
        return True, None
    return False, "KeyNotMatch"

def checkValidity(periods, time):
    currTime = datetime.datetime.strptime(time, "%Y%m%d %H:%M:%S")
    for period in periods:
        notBefore = datetime.datetime.strptime(period[0], "%Y%m%d-%H")
        notAfter = datetime.datetime.strptime(period[1], "%Y%m%d-%H")

        if currTime < notBefore:
            return False
        if currTime > notAfter:
            return False

    return True

def chainValid(usage, time, certs):

    hashList = []
    for crt in certs:
        pem = base64.b64decode(crt)
        hashed = hashlib.sha256(pem).hexdigest()
        hashList.append(hashed)
    key = tuple(hashList)

    if usage == '0' or usage == '1':
        if key in chainMap:
            validity = checkValidity(chainMap[key]['periods'], time)
            if validity:
                return chainMap[key]['usage0'], None
            else:
                return False, "InvalidTime"
        else:
            return None, "NoKey"

    elif usage == '2':
        if len(certs) == 1:
            return False, "NoChain"
        else:
            if key in chainMap:
                validity = checkValidity(chainMap[key]['periods'], time)
                if validity:
                    return chainMap[key]['usage2'], None
                else:
                    return False, "InvalidTime"
            else:
                return None, "NoKey" 
    else:
        return False, "WrongUsage"
    
def findRoot(cert):
    from OpenSSL import crypto
    crt = base64.b64decode(cert)
    crt = crypto.load_certificate(crypto.FILETYPE_PEM, crt)
    issuer = crt.get_issuer()

    if issuer == None:
        return None

    roots = set(root_certs.keys())

    if issuer.CN in roots:
        root = base64.b64encode(root_certs[issuer.CN].encode())
        return root
    return None

def isRoot(cert):
    from OpenSSL import crypto
    crt = base64.b64decode(cert)
    crt = crypto.load_certificate(crypto.FILETYPE_PEM, crt)
    issuer = crt.get_issuer()
    subject = crt.get_subject()
    
    if issuer == None or subject == None:
        return False

    if issuer.CN == subject.CN:
        return True
    return False

def unitValidate_old(record, time, certs):

    from OpenSSL import crypto

    record = record.split()
    if len(record) != 4:
        record = record[:3] + [''.join(record[3:])]
    usage = record[0]
    selector = record[1]
    matching = record[2]
    data = record[3]
  
    error = None
    chain = None
    if usage == '1' or usage == '3':
        
        pem = base64.b64decode(certs[0])
        crt = crypto.load_certificate(crypto.FILETYPE_PEM, pem)
        # use entire certificate
        if selector == '0':
            crt = crypto.dump_certificate(crypto.FILETYPE_ASN1, crt)
            matched, error = matchCrt(matching, data, crt)
            if usage == '1':
                chain, error = chainValid(usage, time, certs)
            return matched, chain, error

        # use public key
        elif selector == '1':
            pubKey = crt.get_pubkey()
            pubKey = crypto.dump_publickey(crypto.FILETYPE_ASN1, pubKey)
            matched, error = matchKey(matching, data, pubKey)
            if usage == '1':
                chain, error = chainValid(usage, time, certs)
            return matched, chain, error
        else:
            return False, chain, "Selector-"+selector
    elif usage == '0' or usage == '2':
        for cert in certs:
            pem = base64.b64decode(cert)
            crt = crypto.load_certificate(crypto.FILETYPE_PEM, pem)

            # use entire certificate
            if selector == '0':
                crt = crypto.dump_certificate(crypto.FILETYPE_ASN1, crt)
                matched, error = matchCrt(matching, data, crt)
                if matched: 
                    chain, error = chainValid(usage, time, certs)
                    return matched, chain, None
            elif selector == '1':
                pubKey = crt.get_pubkey()
                pubKey = crypto.dump_publickey(crypto.FILETYPE_ASN1, pubKey)
                matched, error = matchKey(matching, data, pubKey)
                if matched: 
                    chain, error = chainValid(usage, time, certs)
                    return matched, chain, None
            else:
                return False, None, "Selector-"+selector
        return False, None, "NotInChain"
    else:
        return False, chain, "Usage-"+usage

def unitValidate(record, time, certs):

    from OpenSSL import crypto

    record = record.split()
    if len(record) != 4:
        record = record[:3] + [''.join(record[3:])]
    usage = record[0]
    selector = record[1]
    matching = record[2]
    data = record[3]
  
    error = None
    chain = "Empty"
    if usage == '1' or usage == '3':
        
        pem = base64.b64decode(certs[0])
        crt = crypto.load_certificate(crypto.FILETYPE_PEM, pem)
        # use entire certificate
        if selector == '0':
            crt = crypto.dump_certificate(crypto.FILETYPE_ASN1, crt)
            matched, error = matchCrt(matching, data, crt)
            if usage == '1':
                chain, error = chainValid(usage, time, certs)
                if error == "NoKey":
                    chain = "NoKey"
            return matched, chain, error

        # use public key
        elif selector == '1':
            pubKey = crt.get_pubkey()
            pubKey = crypto.dump_publickey(crypto.FILETYPE_ASN1, pubKey)
            matched, error = matchKey(matching, data, pubKey)
            if usage == '1':
                chain, error = chainValid(usage, time, certs)
                if error == "NoKey":
                    chain = "NoKey"
            return matched, chain, error
        else:
            return False, chain, "Selector-"+selector
    elif usage == '0':
        if isRoot(certs[-1]):
            tmpCerts = certs
        else:
            root = findRoot(certs[-1])
            if root == None:
                tmpCerts = certs
            else:
                tmpCerts = certs + [root]
        for cert in tmpCerts[1:]:
            pem = base64.b64decode(cert)
            crt = crypto.load_certificate(crypto.FILETYPE_PEM, pem)

            chain, error = chainValid(usage, time, certs)
            if error == "NoKey":
                chain = "NoKey"
            if selector == '0':
                crt = crypto.dump_certificate(crypto.FILETYPE_ASN1, crt)
                matched, error = matchCrt(matching, data, crt)
                if matched:
                    return matched, chain, error
            elif selector == '1':
                pubKey = crt.get_pubkey()
                pubKey = crypto.dump_publickey(crypto.FILETYPE_ASN1, pubKey)
                matched, error = matchKey(matching, data, pubKey)
                if matched:
                    return matched, chain, error
            else:
                return False, chain, "Selector-"+selctor
    
        return False, chain, error
    elif usage == '2':

        pem = base64.b64decode(certs[-1])
        crt = crypto.load_certificate(crypto.FILETYPE_PEM, pem)

        chain, error = chainValid(usage, time, certs)
        if error == "NoKey":
            chain = "NoKey"

        # use entire certificate
        if selector == '0':
            crt = crypto.dump_certificate(crypto.FILETYPE_ASN1, crt)
            matched, error = matchCrt(matching, data, crt)
            return matched, chain, error
        elif selector == '1':
            pubKey = crt.get_pubkey()
            pubKey = crypto.dump_publickey(crypto.FILETYPE_ASN1, pubKey)
            matched, error = matchKey(matching, data, pubKey)
            return matched, chain, error
        else:
            return False, chain, "Selector-"+selector
    else:
        return False, False, "Usage-"+usage

def resultCase(dnssecRaw, matched, chain):
    if dnssecRaw == "Secure":
        dnssec = True
    else:
        dnssec = False

    if dnssec:
        if matched:
            if chain: return 0
            elif chain == "Empty": return 0
            else: return 1
        else:
            if chain: return 2
            elif chain == "Empty": return 3
            else: return 4
    else:
        if matched:
            if chain: return 5
            elif chain == "Empty": return 6
            else: return 7
        else:
            if chain: return 8
            elif chain == "Empty": return 9
            else: return 10


def daneValid(d):
    from OpenSSL import crypto
    '''
    #Return
    -1: TLSA record does not exist
    -2: STARTTLS cert does not exist
    '''

    if not ('tlsa' in d):
        return -1
    if not ('certs' in d['starttls']):
        return -2

    dnssec = d['tlsa']['dnssec']
    records = parseTLSA(d['tlsa']['record_raw'])
    time = d['time']+":01:00"

    resultList = []
    for record in records:
        matched, chain, error = unitValidate(record, time, d['starttls']['certs'])
        result = resultCase(dnssec, matched, chain)
        resultList.append(str(result))
    
    return ' '.join(resultList)

def dnskeyExist(d):
    '''
    #Return
    -1: None (DNSSEC is not Insecure)
    0: DNSKEY does not exist
    1: DNSKEY exist
    '''
    
    if not ('tlsa' in d):
        return -1
    if not (d['tlsa']['dnssec'] == "Insecure"):
        return -1

    msg = base64.b64decode(d['tlsa']['record_raw'])
    msg = mymessage.from_wire(msg)
    raw = msg.to_text()

    dn = d['domain']
    query = dn + " .* RRSIG TLSA .*"
    exist = re.search(query, raw)
    if exist:
        return 1

    query = dn + " .* CNAME .*"
    exist = re.search(query, raw)
    if exit:
        query2 = dn + " .* RRSIG CNAME .*"
        exist2 = re.search(query2, raw)
        if exist2:
            return 1
    
    return 0

def initChainMap():
    global chainMap

    chainMap = {}


def getChains():
    global chainMap
    files = os.listdir(chain_path)
    if "_SUCCESS" in files:
        files.remove("_SUCCESS")
    
    initChainMap()

    for filename in files:
        f = open(os.path.join(chain_path, filename), "r")
        while True:
            line = f.readline()
            if not line: break
            line = line[:-1]
            
            if line == "None":
                continue
            line = line.split()
            
            data = line[:-2]
            if line[-2] == "True":
                usage0Result = True
            else:
                usage0Result = False
            if line[-1] == "True":
                usage2Result = True
            else:
                usage2Result = False
            
            certs = tuple(data[0::3])
            periods = list(zip(data[1::3], data[2::3]))
            if certs in chainMap:
                print("certExist!!!!")
                #exit()
            else:
                chainMap[certs] = {"periods":periods, "usage0":bool(usage0Result), "usage2":bool(usage2Result)}
    
        f.close()

def getRootCerts():
    global root_certs

    files = os.listdir(cert_path)
    if "java" in files:
        files.remove("java")

    for filename in files:
        f = open(cert_path + filename, "r")
        cert = f.read()
        crt = crypto.load_certificate(crypto.FILETYPE_PEM, cert)
        issuer = crt.get_issuer().CN

        root_certs[issuer] = cert
        f.close()

def getStats(sc):
    global root_certs

    getRootCerts()

    getChains()
    k = sc.textFile(input_path)\
            .map(tojson)\
            .map(lambda x: (x['domain'], x['port'], x['time'], x['city'],\
            tlsaCrawled(x), starttlsCrawled(x), getStarttlsErr(x),\
            dnssec(x), dnskeyExist(x), daneValid(x)))\
            .map(toCSV)
   
    k.saveAsTextFile(output_path)


if __name__ == "__main__":
    sc = SparkContext(appName="DANE-VALIDATION")
    sc.addPyFile(dependency_path)
    getStats(sc)
    sc.stop()

