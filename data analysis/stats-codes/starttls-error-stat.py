import os
import sys

input_path = "/home/rambotcc/TCC/análise-dados/spark-codes/starttls_error_saopaulo" # path to result of starttls-error.py, ex) /home/ubuntu/starttls_error_virgnia/
output_path = "starttls-error-stat-saopaulo.txt" # ex) /home/ubuntu/starttls-error-stat-virginia.txt



def writeStat(dateMap):
    
    keys = list(dateMap.keys())
    keys = sorted(keys, key=lambda e: (e.split()[0],int(e.split()[1])))

    fOut = open(output_path, "w")
    fOut.write("#date, total, crawled, notCrawled, 4XX, 50X, 55X, tcp, tls, 1XX, 2XX, 3XX, etc\n")

    for date in keys:
        fOut.write(date + "," + str(dateMap[date]['total']) + "," + str(dateMap[date]['crawled']) + "," + str(dateMap[date]['total']-dateMap[date]['crawled']) + "," + str(dateMap[date]['0']) + "," + str(dateMap[date]['1']) +"," + str(dateMap[date]['2']) + "," + str(dateMap[date]['3']) +"," + str(dateMap[date]['4']) + "," + str(dateMap[date]['5']) + "," + str(dateMap[date]['6']) + "," + str(dateMap[date]['7']) +"," + str(dateMap[date]['8']) +"," + "\n")

    fOut.close()

if __name__ == "__main__":
    
    files = os.listdir(input_path)
    if "_SUCCESS" in files:
        files.remove("_SUCCESS")

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
                dateMap[date] = {"total":0, "crawled":0, "0":0, "1":0, "2":0, "3":0, "4":0, "5":0, "6":0, "7":0, "8":0}

            code = line[4]

            dateMap[date]['total'] += 1
            if code == "-1":
                dateMap[date]['crawled'] += 1
                continue
            if code == "-2":
                dateMap[date]["5"] += 1
                continue

            dateMap[date][code] += 1
        f.close()


    writeStat(dateMap)
