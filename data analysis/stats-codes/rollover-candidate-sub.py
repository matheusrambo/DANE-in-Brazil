import os
from os import listdir
import sys

input_path = "" # path to output of rollover-candidate.py, # ex. /home/ubuntu/rollover_cand_output_virginia/
output_path = "" # ex) /home/ubuntu/rollover-candidate-virginia.txt


def getDn():
    files = listdir(input_path)
    if "_SUCCESS" in files:
        files.remove("_SUCCESS")

    totalN = 0
    weirdN = 0
    keyChangedSet = set([])
    
    manySet = set([])
    result = [0, 0, 0, 0, 0, 0, 0, 0]
    for filename in files:
        f = open(os.path.join(input_path, filename), "r")
        while True:
            line = f.readline()
            if not line: break

            line = line[:-1]
            line = line.split()
            totalN += 1

            if line[1] == "NotExistWholePeriod":
                result[0] += 1
            elif line[1] == "NotLetsEncrypt":
                result[1] += 1
            else:
                tlsaChanged = int(line[1])
                certChanged = int(line[2])
                keyChanged = int(line[3])

                if tlsaChanged == 0:
                    if certChanged == 0:
                        result[2] += 1
                    else:
                        if keyChanged == 0:
                            result[3] += 1
                        else:
                            result[4] += 1
                            keyChangedSet.add(line[0])
                else:
                    if certChanged == 0:
                        result[5] += 1
                    else:
                        if keyChanged == 0:
                            result[6] += 1
                        else:
                            result[7] += 1
                            keyChangedSet.add(line[0])

        f.close()


    return totalN, weirdN, result, keyChangedSet


if __name__ == "__main__":

    totalN, weirdN, result, keyChangedSet = getDn()

    whole = totalN - result[0]

    print("Total domains:", totalN)
    print("Do not exist whole period:", result[0])
    print("Exist whole period:", whole)
    print("Target domains:", result[4] + result[7])
    print("\n- Details")
    print("TLSA X, Cert X, Key -:", result[2], str(round(100* result[2]/whole, 2)) + "%")
    print("TLSA X, Cert O, Key X:" ,result[3], str(round(100* result[3]/whole, 2)) + "%")
    print("TLSA X, Cert O, Key O:", result[4], str(round(100* result[4]/whole, 2)) + "%")
    print("TLSA O, Cert X, Key -:", result[5], str(round(100* result[5]/whole, 2)) + "%")
    print("TLSA O, Cert O, Key X:", result[6], str(round(100* result[6]/whole, 2)) + "%")
    print("TLSA O, Cert O, Key O:", result[7], str(round(100* result[7]/whole, 2)) + "%")

    fOut = open(output_path, "w")
    for dn in keyChangedSet:
        fOut.write(dn + "\n")
    fOut.close()

