import os

input_path = "/home/rambotcc/TCC/análise-dados/spark-codes/valid_dn_saopaulo" # path to result of valid-dn.py, ex. /home/ubuntu/valid_dn_output_virginia/
output_path = "valid-dn-stat-saopaulo.txt" # /home/ubuntu/valid-dn-stat-virginia.txt


def writeStat(dateMap):

    keys = list(dateMap.keys())
    keys = sorted(keys, key=lambda e: (e.split()[0], int(e.split()[1])))

    fOut = open(output_path, "w")
    fOut.write("time, total_br_dn, valid_br_dn, invalid_br_dn\n")
    tlds = ["br"]
    for key in keys:
        results = dateMap[key]
    
        output = key
        for tld in tlds:
            output = output + "," + str(results[tld][0]) + "," + str(results[tld][1]) + "," + str(results[tld][2])
        fOut.write(output + "\n")

    fOut.close()


def getStat():
    files = os.listdir(input_path)

    if "_SUCCESS" in files:
        files.remove("_SUCCESS")

    tlds = ["br"]
    dateMap = {}
    valid = 0
    invalid = 0 
    for filename in files:
        f = open(os.path.join(input_path, filename), "r")
        while True:
            line = f.readline()
            if not line: break

            line = line[:-1]
            line = line.split()

            time = line[1] + " " + line[2]
            
            if not time in dateMap:
                dateMap[time] = {"br":[0, 0, 0]}

            results = line[3:]
            print(results)
            if results == []:
                continue
            if line[3] == "NoData":
                continue
            it = iter(results)
            for x in it:
                
                tld = x
                isValid = next(it)
                num = int(next(it))

                dateMap[time][tld][0] += num
                if isValid == "1":
                    dateMap[time][tld][1] += num
                else:
                    dateMap[time][tld][2] += num
        f.close()
    writeStat(dateMap)

        
if __name__ == "__main__":

    getStat()
