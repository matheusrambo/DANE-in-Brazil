import json
import time
filename = time.strftime("%Y-%m-%d")
output = open('saida/'+ filename + '-dns.txt', 'w+')
with open('entrada/' + filename + '-dns.json') as json_file:
    entrada = json_file.readlines()

#output = open('saida/3dominios-teste-dns.txt', 'w+')
#with open('entrada/3dominios-teste-dns.json') as json_file:
#	entrada = json_file.readlines()

for i in range (len(entrada)):
    data = (json.loads(entrada[i]))
    cont = 0
    for i in range(0,10): #contar quantos MXs possue em tal dominio
        try:
            if (data['results']['MAIL'][i]['host']) != "":
                cont = cont + 1
        except:
            cont = cont
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
            pass
        else:    
            linha = "_" + porta + "._tcp." + mx + "," + str(usage) + "," + str(selector) + "," + str(matchingtype) + "," + data_key + ",1\n"
            print(linha)
            output.write(linha)


