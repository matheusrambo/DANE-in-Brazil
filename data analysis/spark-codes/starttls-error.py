#!/user/bin/python3
from pyspark import SparkContext, StorageLevel
from pyspark import SparkConf

import os
import dns as mydns
import dns.message as mymessage
import base64
import json
import re


dependency_path = "/home/rambotcc/TCC/análise-dados/spark-codes/dependencies.zip" 
input_path = "/home/rambotcc/TCC/raw-merge/merged_output/saopaulo"  
output_path = "/home/rambotcc/TCC/análise-dados/spark-codes/starttls_error" 



def tojson(line):
    return json.loads(line)

def toCSV(line):
    return " ".join(str(d) for d in line)

def getStarttlsErr(data):
    '''
    #Return
    -2: NoData
    -1: None (STARTTLS certificate is crawled)
    
    case0: 4* 
    case1: 50*
    case2: 55*
    case3: tcp
    case4: tls
    case5: 1*
    case6: 2*
    csee7: 3*
    case8: etc
    '''

    case0 = ["4"]
    case1 = ["500", "502", "503", "504"]
    case2 = ["551", "550", "554"]
    case3 = ["dial tcp", "write tcp", "read tcp"]
    case4 = ["remote error", "tls"]
    case5 = ["1"]
    case6 = ["2"]
    case7 = ["3"]
    
    if 'why_fail' in data['starttls']:
        why_fail = data['starttls']['why_fail']
        domain = data['domain']
        for code in case0:
            codeLen = len(code)
            if why_fail[:codeLen] == code:
               return "0"

        for code in case1:
            codeLen = len(code)
            if why_fail[:codeLen] == code:
                return "1"
        for code in case2:
            codeLen = len(code)
            if why_fail[:codeLen] == code:
                return "2 " 
        for code in case3:
            codeLen = len(code)
            if why_fail[:codeLen] == code:
                return "3 " + why_fail
        for code in case4:
            codeLen = len(code)
            if why_fail[:codeLen] == code:
                return "4 " + why_fail
        for code in case5:
            codeLen = len(code)
            if why_fail[:codeLen] == code:
                return "5 "
        for code in case6:
            codeLen = len(code)
            if why_fail[:codeLen] == code:
                return "6 "
        for code in case7:
            codeLen = len(code)
            if why_fail[:codeLen] == code:
                return "7 "

        if why_fail == "NoData":
            return -2
        return "8 " + why_fail
    return -1

def getStats(sc):
    k = sc.textFile(input_path)\
            .map(tojson)\
            .map(lambda x: (x['domain'], x['port'], x['time'], getStarttlsErr(x)))\
            .map(toCSV)
    
    k.saveAsTextFile(output_path)


if __name__ == "__main__":
    sc = SparkContext(appName="DANE-RESEARCH-DNSSEC-STAT")
    sc.addPyFile(dependency_path)
    getStats(sc)
    sc.stop()
