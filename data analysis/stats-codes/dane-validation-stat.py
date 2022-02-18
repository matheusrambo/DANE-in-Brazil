import os
from os import listdir
import sys

input_path = "/home/rambotcc/TCC/análise-dados/spark-codes/dane_output_saopaulo" # path to results of dane-validation.py, # ex. /home/ubuntu/dane_validation_virginia/
output_path = "dane-validation-stat-saopaulo.txt" # ex) /home/ubuntu/dane-validation-stat-virginia.txt


def writeResult(statMap):

    times = statMap.keys()
    times = sorted(times, key=lambda e: (int(e.split()[0]), int(e.split()[1])))

    fOut = open(output_path, "w")
    fOut.write("#date, total, tlsaCrawled, certCralwed, secure, insecure, bogus, w-rrsig-wo-ds, wo-rrsig-wo-ds, daneValid, daneInvalid, chainOnlyErr, matchingOnlyErr, dnssecOnlyErr, moreThanTwoErrs, chainTotal, matchingTotal, dnssecTotal\n")
    for time in times:
        data = statMap[time]
        date = time.split()[0]
        date = date[:4] + "-" + date[4:6] + "-" + date[6:]
        hour = time.split()[1]
        fOut.write(date+" "+hour+","+str(data["total"])+","+str(data["tlsaCrawled"])+","+str(data["certCrawled"])+","+str(data["secure"])+","+str(data["insecure"])+","+str(data["bogus"])+","+str(data["keyExist"])+","+str(data["keyNotExist"])+","+str(data["daneValid"])+","+str(data["daneInvalid"])+","+str(data["chainErr"])+","+str(data["matchingErr"])+","+str(data["dnssecErr"])+","+str(data["complicateErr"])+","+str(data["chainTotal"])+","+str(data["matchingTotal"])+","+str(data["dnssecTotal"])+"\n")

    fOut.close()

    
if __name__ == "__main__":

    files = listdir(input_path)
    if "_SUCCESS" in files:
        files.remove("_SUCCESS")

    statMap = {}
    for filename in files:
        f = open(os.path.join(input_path, filename), "r")

        while True:
            line = f.readline()
            if not line: break
            line = line[:-1].split()
            
            time = line[2] + " " + line[3]

            tlsaCrawled = line[5]
            certCrawled = line[6]
            certErr = line[7]
            dnssec = line[8]
            keyExist = line[9]
            results = set(line[10:])

            if not (time in statMap):
                statMap[time] = {"total":0, "tlsaCrawled":0, "certCrawled":0, "err4":0, "err5":0, "errUnknown":0, "errConn":0, "secure":0, "insecure":0, "bogus":0, "keyExist":0, "keyNotExist":0, "daneValid":0, "daneInvalid":0, "chainErr":0, "matchingErr":0, "dnssecErr":0, "complicateErr":0, "chainTotal":0, "matchingTotal":0, "dnssecTotal":0}
            curr = statMap[time]
            curr["total"] += 1
            if tlsaCrawled == "1":
                curr["tlsaCrawled"] += 1

                if dnssec == "0":
                    curr["secure"] += 1
                elif dnssec == "1":
                    curr["insecure"] += 1
                    if keyExist == "1":
                        curr["keyExist"] +=1
                    elif keyExist == "0":
                        curr["keyNotExist"] += 1
                    else:
                        continue
                elif dnssec == "2":
                    curr["bogus"] += 1
                else:
                    continue
            
            if certCrawled == "1":
                curr["certCrawled"] += 1
            else:
                if certErr == "0":
                    curr["err4"] += 1
                elif certErr == "1":
                    curr["err5"] += 1
                elif certErr == "2":
                    curr["errUnknown"] += 1
                elif certErr == "3":
                    curr["errConn"] += 1
                elif certErr == "-1":
                    curr["errConn"] += 1
                elif certErr == "-2":
                    curr["errConn"] += 1
                else:
                    continue

            if tlsaCrawled == "0" or certCrawled == "0":
                continue
    
            if "0" in results:
                curr["daneValid"] += 1
            else:
                curr["daneInvalid"] += 1
                resultsSet = set(results)

                if len(resultsSet) == 1:
                    if "5" in resultsSet or "6" in resultsSet:
                        curr["dnssecErr"] += 1
                    elif "2" in resultsSet or "3" in resultsSet:
                        if not ("1" in resultsSet or "5" in resultsSet or "6" in resultsSet or "7" in resultsSet):
                            curr["matchingErr"] += 1
                    elif "1" in resultsSet:
                        curr["chainErr"] += 1
                    else:
                        curr["complicateErr"] += 1
                elif len(resultsSet) == 2:
                    if "5" in resultsSet and "6" in resultsSet:
                        curr["dnssecErr"] += 1
                    elif "2" in resultsSet and "3" in resultsSet:
                        if not ("1" in resultsSet or "5" in resultsSet or "6" in resultsSet or "7" in resultsSet):
                            curr["matchingErr"] += 1
                    else:
                        curr["complicateErr"] += 1
                else:
                    curr["complicateErr"] += 1

                if "1" in results or "4" in results or "7" in results or "10" in results:
                    curr["chainTotal"] += 1
                if "2" in results or "3" in results or "4" in results or "8" in results or "9" in results or "10" in results:
                    curr["matchingTotal"] += 1
                if "5" in results or "6" in results or "7" in results or "8" in results or "9" in results or "10" in results:
                    curr["dnssecTotal"] += 1

        f.close()

    writeResult(statMap)

