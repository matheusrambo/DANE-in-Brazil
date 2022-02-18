import os
from os import listdir
import sys

output_path = "dnssec-stat-saopaulo.txt" 
input_path = "/home/rambotcc/TCC/análise-dados/spark-codes/dnssec_output" # path to results of dnssec.py


def getStats():
    files = listdir(input_path)
    if "_SUCCESS" in files:
        files.remove("_SUCCESS")

    timeMap = {}
    for filename in files:
        f  = open(os.path.join(input_path, filename) , "r")

        while True:
            line = f.readline()
            print(line)
            if not line:break
            line = line[:-1]
            origin = line
            line = line.split()
            
            time = line[2] + " " + line[3]
            code = line[4]
            if time in timeMap:
                timeMap[time]['total'] += 1
                timeMap[time][code] += 1
                if code == '3':
                    if '13,' in line[5]:
                        bogusCode = 'b13'
                        timeMap[time][bogusCode] += 1
                    else:
                        bogusCode = 'b' + line[5]
                        timeMap[time][bogusCode] += 1

            else:
                timeMap[time] = {'total':1, '-1':0, '0':0, '1':0, '2':0, '3':0, 'b0':0, 'b1':0, 'b2':0, 'b3':0, 'b5':0, 'b4':0, 'b6':0, 'b7':0, 'b8':0, 'b9':0, 'b10':0, 'b11':0, 'b12':0, 'b13':0}
                timeMap[time][code] += 1


        f.close()

    keys = list(timeMap.keys())
    keys = sorted(keys, key=lambda e: (e.split()[0], int(e.split()[1])))

    fOut = open(output_path, "w")
    fOut.write("#time, totalDomain, failed-to-fetch, secure, wo-rrsig-wo-ds, w-rrsig-wo-ds, bogus, signature_expired, no_dnskey, no_signatures, no_ds, no_dnssec, DS_mismatch, wildcard_proof_failed, signature_crypto_failed, signature_missing, cnmae_proof_failed, signature_before_inception_date, signature_from_unknown_keys, covering_NSEC3_was_not_opt-out_in_an_opt-out_DS_NOERROR/NODATA' \n")
    for key in keys:
        fOut.write(key + "," + str(timeMap[key]['total']) + "," + str(timeMap[key]['-1']) + "," + str(timeMap[key]['0']) +"," + str(timeMap[key]['1']) + "," + str(timeMap[key]['2']) + "," + str(timeMap[key]['3']) + "," + str(timeMap[key]['b0']) + "," + str(timeMap[key]['b1'] + timeMap[key]['b4']) + "," + str(timeMap[key]['b6'] + timeMap[key]['b10']) + "," + str(timeMap[key]['b8']) + "," + str(timeMap[key]['b12']) + "," + str(timeMap[key]['b2']) + "," + str(timeMap[key]['b3']) + "," + str(timeMap[key]['b5']) + ","+ str(timeMap[key]['b7']) + "," + str(timeMap[key]['b9']) + "," + str(timeMap[key]['b11']) + "," + str(timeMap[key]['b11']) + "," + str(timeMap[key]['b13'])+ "\n")
    fOut.close()

if __name__ == "__main__":
    getStats()
