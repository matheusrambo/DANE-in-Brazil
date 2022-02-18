#!/user/bin/python3
from pyspark import SparkContext

import sys, os
import dns as mydns
import dns.message as mymessage
import base64
from OpenSSL import crypto
import json
import hashlib


dependency_path = "/home/rambotcc/TCC/análise-dados/spark-codes/dependencies.zip" # ex) path to 'dependencies.zip'
input_path = "/home/rambotcc/TCC/raw-merge/merged_output/saopaulo"  # ex) /user/hadoop/data/virginia
output_path = "/home/rambotcc/TCC/análise-dados/spark-codes/incorrect_output_saopaulo" # ex) /user/hadoop/incorrect_output_virginia


cert_path = "/etc/ssl/certs/" # Path of root CA certificates in Ubuntu
root_certs = {}

def tojson(line):
    return json.loads(line)

def toCSV(line):
    return " ".join(str(d) for d in line)


def parseTLSA(encoded):
    msg = base64.b64decode(encoded)
    msg = mymessage.from_wire(msg)
    rrset = []

    for answer in msg.answer:
        if answer.rdtype == 52:
            rrset = rrset + [data.to_text() for data in answer]
    return rrset

def checkWeirdField(records):
    def checkUsage(x):
        if x == '0' or x =='1' or x == '2' or x =='3':
            return True, None
        else:
            return False, x
    def checkSelector(x):
        if x == '0' or x == '1':
            return True, None
        else:
            return False, x
    def checkMatchingType(x):
        if x == '0' or x == '1' or x == '2':
            return True, None
        else:
            return False, x
    
    # Check Incorrect Field Values
    results = [None, None, None]
    for record in records:
        record = record.split()
        if len(record) != 4:
            record = record[:4] + [''.join(record[3:])]
        usage = record[0]
        selector = record[1]
        matching = record[2]

        usageValid, wrongUsage = checkUsage(usage)
        selectorValid, wrongSelector = checkSelector(selector)
        matchingValid, wrongMatching = checkMatchingType(matching)

        if not usageValid:
            results[0] = wrongUsage
        if not selectorValid:
            results[1] = wrongSelector
        if not matchingValid:
            results[2] = wrongMatching

    if not len(set(results)) == 1:
        return results
    else:
        if not (None in results):
            return results
    
    return None

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
        # Must not enter her
        return None, "Matching-"+matching

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
        # Must not enter here
        return None, "Matching-"+matching

    if hashed == data:
        return True, None
    return False, "KeyNotMatch"


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


def unitValidate(record, certs):
    from OpenSSL import crypto
    record = record.split()
    if len(record) != 4:
        record = record[:4] + [''.join(record[3:])]
    usage = record[0]
    selector = record[1]
    matching = record[2]
    data = record[3]

    error = None
    matched = False
    if usage == '0':
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

            if selector == '0':
                crt = crypto.dump_certificate(crypto.FILETYPE_ASN1, crt)
                matched, error = matchCrt(matching, data, crt)
                if matched:
                    return matched, error
            elif selector == '1':
                pubKey = crt.get_pubkey()
                pubKey = crypto.dump_publickey(crypto.FILETYPE_ASN1, pubKey)
                matched, error = matchKey(matching, data, pubKey)
                if matched:
                    return matched, error
            else:
                # Must not enter here
                return None, None
        return False, error
    elif usage == '2':
        pem = base64.b64decode(certs[-1])
        crt = crypto.load_certificate(crypto.FILETYPE_PEM, pem)

        if selector == '0':
            crt = crypto.dump_certificate(crypto.FILETYPE_ASN1, crt)
            matched, error = matchCrt(matching, data, crt)
        elif selector == '1':
            pubKey = crt.get_pubkey()
            pubKey = crypto.dump_publickey(crypto.FILETYPE_ASN1, pubKey)
            matched, error = matchKey(matching, data, pubKey)
        else:
            # Must not enter here
            return None, None
        return matched, error
    elif usage == '1' or usage == '3':
        pem = base64.b64decode(certs[0])
        crt = crypto.load_certificate(crypto.FILETYPE_PEM, pem)

        if selector == '0':
            crt = crypto.dump_certificate(crypto.FILETYPE_ASN1, crt)
            matched, error = matchCrt(matching, data, crt)
        elif selector == '1':
            pubKey = crt.get_pubkey()
            pubKey = crypto.dump_publickey(crypto.FILETYPE_ASN1, pubKey)
            matched, error = matchKey(matching, data, pubKey)
        else:
            # Must not enter here
            return None, None
        return matched, error
    else:
        #Must not enter here
        return None, None

def findMatch(record, certs):
    from OpenSSL import crypto

    record = record.split()
    if len(record) != 4:
        record = record[:3] + [''.join(record[3:])]
    usage = record[0]
    selector = record[1]
    matching = record[2]
    data = record[3]

    tmpCerts = certs
    if usage == '0':
        if isRoot(certs[-1]):
            tmpCerts = certs
        else:
            root = findRoot(certs[-1])
            if root == None:
                tmpCerts = certs
            else:
                tmpCerts = certs + [root]
 

    for index, cert in enumerate(tmpCerts):
        idx = str(index+1) + "/" + str(len(tmpCerts))
        pem = base64.b64decode(cert)
        crt = crypto.load_certificate(crypto.FILETYPE_PEM, pem)

        key = crt.get_pubkey()
        key = crypto.dump_publickey(crypto.FILETYPE_ASN1, key)
        crt = crypto.dump_certificate(crypto.FILETYPE_ASN1, crt)

        key256 = hashlib.sha256(key).hexdigest()
        if data.lower() == key256:
            return "key256 " + idx + " " + key256
        key512 = hashlib.sha512(key).hexdigest()
        if data.lower() == key512:
            return "key512 " + idx + " " + key512
        crt256 = hashlib.sha256(crt).hexdigest()
        if data.lower() == crt256:
            return "crt256 " + idx + " " + crt256
        crt512 = hashlib.sha512(crt).hexdigest()
        if data.lower() == crt512:
            return "crt512 " + idx + " " + crt512

        crt_raw = crypto.load_certificate(crypto.FILETYPE_ASN1, crt)
        crt_raw = crypto.dump_certificate(crypto.FILETYPE_PEM, crt_raw).lower().decode()
        crt_raw = crt_raw.replace('-----begin certificate-----', '').replace('-----end certificate-----', '').replace('\n', '').lower()
        
        key_raw = crypto.load_publickey(crypto.FILETYPE_ASN1, key)
        key_raw = crypto.dump_publickey(crypto.FILETYPE_PEM, key_raw).lower().decode()
        key_raw = key_raw.replace('-----begin public key-----', '').replace('-----end public key-----', '').replace('\n', '').lower()
        
        data = data.replace('-----begin certificate-----', '').replace('-----end certificate-----' ,'').replace('-----begin public key-----', '').replace('-----end public key-----', '').replace('\n', '').lower()

        if crt_raw == data:
            return "crtRaw " + idx + " " + crt_raw
        if key_raw == data:
            return "keyRaw " + idx + " " + key_raw
    return None

def checkIncorrectField(records, certs):
    # First remove correct data
    for record in records:
        matched, error = unitValidate(record, certs)
        if matched:
            return "Correct"
    # among incorrect ones, find field - association data inconsistency
    caseList = []
    for record in records:
        case = findMatch(record, certs)
        if case == None:
            continue
        caseList.append(record + " " + case)

    if caseList == []:
        return "NoCase"

    return caseList
    

def Incorrectness(data):
    
    if not 'tlsa' in data:
        return (data['domain'], data['port'], data['time'], "NoTLSA")

    if not 'certs' in data['starttls']:
        return (data['domain'], data['port'], data['time'], "NoCerts")

    if data['tlsa']['dnssec'] == "Insecure":
        return (data['domain'], data['port'], data['time'], "NoDS")


    records = parseTLSA(data['tlsa']['record_raw'])
    
    weird = checkWeirdField(records)
    if not weird == None:
        return [data['domain'], data['port'], data['time'], "WeirdField"] + weird
    
    incorrect = checkIncorrectField(records, data['starttls']['certs'])
    if incorrect == "Correct" or incorrect == "NoCase":
        return (data['domain'], data['port'], data['time'], incorrect)
    
    return [data['domain'], data['port'], data['time'], "Incorrect"] + incorrect

def getRootCerts():
    global root_certs

    files = os.listdir(cert_path)
    if "java" in files:
        files.remove("java")

    for filename in files:
        f = open(os.path.join(cert_path, filename), "r")
        cert = f.read()
        crt = crypto.load_certificate(crypto.FILETYPE_PEM, cert)
        issuer = crt.get_issuer().CN

        root_certs[issuer] = cert
        f.close()

def getStats(sc):
    getRootCerts()

    k = sc.textFile(input_path)\
            .map(tojson)\
            .map(Incorrectness)\
            .map(toCSV)

    k.saveAsTextFile(output_path)

if __name__ == "__main__":
    sc = SparkContext(appName="DANE-INCORRECT-CHECK")
    sc.addPyFile(dependency_path)
    getStats(sc)
    sc.stop()
