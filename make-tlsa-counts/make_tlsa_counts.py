import json
import time
import os
import re
###############################
output = open('tlsa.txt', 'w+')
output.write("#date,br_mxcount,br_one_tlsa_sig,br_one_tlsa_unsig,br_all_tlsa_sig,br_all_tlsa_one_unsig\n")
path = "" #path to the folder that has the input files
os.chdir(path) 
teste = []
def read_text_file(file_path): 
    with open(file_path, 'r') as f: 
        print(f.read()) 

for file in os.listdir(): 
    if file.endswith(".json"): 
        file_path = f"{path}/{file}"

        with open(file_path) as json_file:
            entrada = json_file.readlines()

        mxcount = 0
        one_tlsa_sig = 0
        all_tlsa_sig = 0
        for i in range (len(entrada)):
            data = (json.loads(entrada[i]))
            cont = 0
            conta_zero = 0
            for i in range(0,10): #contar quantos MXs possue em tal dominio
                try:
                    if (data['results']['MAIL'][i]['host']) != "":
                        cont = cont + 1
                except:
                    cont = cont
            if(cont != 0):
                mxcount = mxcount + 1

            for i in range (0,cont):
                porta = "25"
                try:
                    mx = data['results']['MAIL'][i]['host']
                    usage = data['results']['MAIL'][i]['TLSA']['25'][0]['usage']
                    selector = data['results']['MAIL'][i]['TLSA']['25'][0]['selector']
                    matchingtype = data['results']['MAIL'][i]['TLSA']['25'][0]['matchingtype']
                    data_key = data['results']['MAIL'][i]['TLSA']['25'][0]['data']
                except:
                    usage = 0
                    selector = 0
                    matchingtype = 0
                    data_key = 0
                if usage==0 or selector==0 or matchingtype==0 or data_key==0:
                    conta_zero = conta_zero + 1
                else: 
                    one_tlsa_sig = one_tlsa_sig + 1

                if conta_zero == 0:
                    all_tlsa_sig = all_tlsa_sig + 1
                else:
                    pass

        print("mxcount:", mxcount)
        print("one_tlsa_sig:", one_tlsa_sig)
        print("all_tlsa_sig:", all_tlsa_sig)
        for i in range(10,18):
            name_file =file.replace("-dns.json","")
        linha = file + "," + str(mxcount) + "," + str(one_tlsa_sig) + ",0," + str(all_tlsa_sig) + ",0\n"
        output.write(linha)   