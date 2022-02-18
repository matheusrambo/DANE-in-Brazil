#!/user/bin/python3
from pyspark import SparkContext, StorageLevel

import csv
import os


dane_output_path = "/home/rambotcc/TCC/análise-dados/spark-codes/dane_output_saopaulo" # path to dane validation result (output of dane-validation.py), ex) /user/ubuntu/output/dane_output_virginia
dependency_path = "/home/rambotcc/TCC/análise-dados/spark-codes/dependencies.zip" # path to 'dependencies.zip'
mxcount_path = "/home/rambotcc/TCC/DNS-crawler/mx-with-TLSA" # path to 'mx-with-tlsa'
output_path = "/home/rambotcc/TCC/análise-dados/spark-codes/valid_dn_saopaulo" # ex) /user/hadoop/valid_dn_virginia



tldMap = {}
tlds = ["br"]

def toCSV(line):
    return " ".join(str(d) for d in line)


def getValidSet():
    global tldMap
    
    for tld in tlds:
        tldPath = os.path.join(mxcount_path, tld)
        files = os.listdir(tldPath)

        dateMap = {}
        for filename in files:
            date = filename.split(".")[0]
            date = date.replace("-", "")
            curr = {}
            f = open(os.path.join(tldPath, filename), "r")
            reader = csv.reader(f)
            next(reader, None)
            for line in reader:
                dn = line[0]
                
                if dn in curr:
                    continue
                num = int(line[5])
                curr[dn] = num
            dateMap[date] = curr
            f.close()

        tldMap[tld] = dateMap

def isValid(d):
    data = d.split()

    results = data[10:]

    dn = "_" + data[1] + "._tcp." + data[0] + "."

    date = data[2] + " " + data[3]

    valids =[]
    if data[5] == "0" or data[6] == "0":
        return dn + " " + date + " NoData"

    if "0" in results:
        for tld in tlds:
            targetDateMap = tldMap[tld][data[2]]
            targetDns = targetDateMap.keys()
            if dn in targetDns:
                valids.append(tld + " 1 " + str(targetDateMap[dn]))
    else:
        for tld in tlds:
            targetDateMap = tldMap[tld][data[2]]
            targetDns = targetDateMap.keys()
            if dn in targetDns:
                valids.append(tld + " -1 " + str(targetDateMap[dn]))
 


    return dn + " " + date + " " + " ".join(valids)


def getStats(sc):
    
    getValidSet()
    k = sc.textFile(dane_output_path)\
            .map(isValid)
    
    k.saveAsTextFile(output_path)


if __name__ == "__main__":
    sc = SparkContext(appName="DANE-RESEARCH-VALID-DN-STAT")
    sc.addPyFile(dependency_path)
    getStats(sc)
    sc.stop()
