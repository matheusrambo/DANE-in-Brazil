import json

output = open('tlsa_0_saida.txt', 'w+')
with open('tlsa_0.json') as json_file:
    entrada = json_file.readlines()

for string in entrada:
    linha = string.split(",")
    
    linha2 = linha[0][:-1] + "," + linha[1] + "," + linha[2] + "," + linha[3]
    print(linha2)
    output.write(linha2)

