#!/user/bin/python3
from pyspark import SparkContext, StorageLevel
from pyspark import SparkConf
import sys
sys.path.insert(0, '/home/rambotcc/TCC/análise-dados/spark-codes/dependencies')

import dns as mydns
import dns.message as mymessage
import base64
import json
import re


dependency_path = "/home/rambotcc/TCC/análise-dados/spark-codes/dependencies.zip" # ex) path to 'dependencies.zip'
input_path = "/home/rambotcc/TCC/raw-merge/merged_output/saopaulo"  # ex) /user/hadoop/data/virginia
output_path = "/home/rambotcc/TCC/análise-dados/spark-codes/dnssec_output" # ex) /user/hadoop/dnssec_output_virginia


def tojson(line):
    return json.loads(line)

def toCSV(line):
    return " ".join(str(d) for d in line)

def whybogus(d):
    if not dnssec(d) == 3:
        return -1

    why_bogus = d['tlsa']['why_bogus']
    if "signature expired" in why_bogus:
        return 0
    elif "no keys have a DS" in why_bogus:
        return 1
    elif "DS hash mismatches" in why_bogus:
        return 2
    elif "wildcard proof failed" in why_bogus:
        return 3
    elif "No DNSKEY record" in why_bogus:
        return 4
    elif "signature crypto failed" in why_bogus:
        return 5
    elif "signature missing from" in why_bogus:
        return 6
    elif "cname proof failed" in why_bogus:
        return 7
    elif "no DS" in why_bogus:
        return 8
    elif "signature before inception date" in why_bogus:
        return 9
    elif "no signatures" in why_bogus:
        return 10
    elif "signatures from unknown keys" in why_bogus:
        return 11
    elif "no DNSSEC records" in why_bogus:
        return 12
    else:
        return 13, why_bogus


def dnskeyExist(dn, encoded):
    '''
    #Return
    0: DNSKEY does not exist
    1: DNSKEY exist
    '''
    
    msg = base64.b64decode(encoded)
    msg = mymessage.from_wire(msg)
    raw = msg.to_text()

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

def dnssec(d):
    '''
    #Return
    -1: None
    0: Secure
    1: Insecure w/o DS w/o DNSKEY
    2: Insecure w/o DS w DNSKEY
    3: Bogus
    4: Error value
    '''

    if 'tlsa' in d:
        dnssec = d['tlsa']['dnssec']
        if dnssec == "Secure":
            return 0
        elif dnssec == "Insecure":
            if dnskeyExist(d['domain'], d['tlsa']['record_raw']):
                return 2
            else:
                return 1
        elif dnssec == "Bogus":
            return 3
        else:
            return 4
    return -1


def getStats(sc):
    k = sc.textFile(input_path)\
            .map(tojson)\
            .map(lambda x: (x['domain'], x['port'], x['time'], dnssec(x), whybogus(x)))\
            .map(toCSV)
    
    k.saveAsTextFile(output_path)


if __name__ == "__main__":
    sc = SparkContext(appName="DANE-RESEARCH-DNSSEC-STAT")
    sc.addPyFile(dependency_path)
    getStats(sc)
    sc.stop()
