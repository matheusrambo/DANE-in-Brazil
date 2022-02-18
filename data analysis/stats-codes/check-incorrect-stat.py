import os
from os import listdir
import sys

dane_result_path = "/home/rambotcc/TCC/análise-dados/stats-codes/dane-validation-stat-saopaulo.txt" # path to 'dane-validation-stat.txt', which means that you need to run dane-validation-stat.py before running this script. Also, must use same city's data
input_path = "/home/rambotcc/TCC/análise-dados/spark-codes/incorrect_output_saopaulo" # path to result of check-incorrect.py, ex) /home/ubuntu/check_incorrect_virgnia/
output_path = "/home/rambotcc/TCC/análise-dados/stats-codes/check-incorrect-stat-saopaulo.txt" # ex) home/ubuntu/check-incorrect-stat-virginia.txt


def getStats():
    files = listdir(input_path)
    if "_SUCCESS" in files:
        files.remove("_SUCCESS")

    total = 0
    noTlsa = 0
    noCerts = 0
    weird = 0
    dateMap = {}
    for filename in files:
        f = open(os.path.join(input_path, filename), "r")
        while True:
            line = f.readline()
            if not line: break
            line = line[:-1]
            line = line.split()

            date = line[2] + " " + line[3]
            if not date in dateMap:
                dateMap[date] = {'total':0, 'noData':0, 'noDs':0, 'cert-tlsaMismatch':0, 'undefined':0, 'incorrect-usage2':0, 'incorrect-usage3':0, 'incorrect-selector':0, 'incorrect-matching':0}

            curr = dateMap[date]
            curr['total'] += 1
            if line[4] == "NoTLSA" or line[4] == "NoCerts":
                curr['noData'] += 1
            elif line[4] == "NoDS":
                curr['noDs'] += 1
            elif line[4] == "NoCase":
                curr['cert-tlsaMismatch'] += 1
            elif line[4] == "WeirdField":
                curr['undefined'] += 1
            elif line[4] == "Incorrect":
                result = line[5:]
                if result[0] == '2':
                    index = result[5].split("/")
                    certLen = index[1]
                    index = index[0]
                    if not index == certLen:
                        curr['incorrect-usage2'] += 1
                else:
                    selector = result[1]
                    if selector == '0':
                        selector = 'crt'
                    else:
                        selector = 'key'
                    matching = result[2]
                    if matching == '0':
                        matching = 'Raw'
                    if matching == '1':
                        matching = '256'
                    else:
                        matching = '512'

                    realSelector = result[4][:3]
                    realMatching = result[4][3:]
                    
                    if not selector == realSelector or not matching == realMatching:
                        if not selector == realSelector:
                            curr['incorrect-selector'] += 1
                        if not matching == realMatching:
                            curr['incorrect-matching'] += 1
                    else:
                        if result[0] == '3':
                            index = result[5].split("/")[0]
                            if not index == '0':
                                curr['incorrect-usage3'] += 1
                        else:
                            continue

        f.close()
    return dateMap


def getBogusAndChain(dateMap):
    f = open(dane_result_path, "r")
    f.readline()
    while True:
        line = f.readline()
        if not line: break
        line = line[:-1]
        line = line.split(",")
        time = line[0].split("-")
        time = "".join(time)

        bogus = int(line[6])
        chain = int(line[15])

        dateMap[time]['bogus'] = bogus
        dateMap[time]['chain'] = chain
    f.close()

    return dateMap



if __name__ == "__main__":

    dateMap = getStats()

    dateMap = getBogusAndChain(dateMap)

    total = 0
    aggregated = 0

    dates = dateMap.keys()
    dates = sorted(dates, key=lambda e: (e.split()[0], int(e.split()[1])))

    fOut = open(output_path, "w")
    fOut.write("#time, totalDn, noData, bogus, wrongChain, usage2, usage3, undefined, selector, matchingType, others(mismatch_cert_tlsa)\n")
    for date in dates:
        data = dateMap[date]
        fOut.write(date + "," + str(data['total']) + "," + str(data['noData']) + "," + str(data['bogus']) + "," + str(data['chain']) + "," + str(data['incorrect-usage2']) + "," + str(data['incorrect-usage3']) + "," + str(data['undefined']) + "," + str(data['incorrect-selector']) + "," + str(data['incorrect-matching']) + "," + str(data['cert-tlsaMismatch']) +"\n")
    fOut.close()
