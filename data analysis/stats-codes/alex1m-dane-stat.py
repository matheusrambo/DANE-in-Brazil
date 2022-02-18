import csv

mx_path = "" # path to alexa_mx_20191031 
tlsa_path = "" # path to alexa_tlsa_20191031
alexa_path = "" # path to alexa-top1m-2019-10-31_0900_UTC.csv

output_path = "" # ex) alexa1m-dane-stat.txt

def getMX():
    f = open(mx_path, "r")

    mxSet = set([])
    while True:
        line = f.readline()
        if not line: break
        line = line[:-1]

        mxSet.add(line)
    f.close()

    return mxSet


def getTLSA():
    f = open(tlsa_path, "r")
    tlsaSet = set([])
    while True:
        line = f.readline()
        if not line: break
        line = line[:-1]

        tlsaSet.add(line)

    f.close()
    return tlsaSet

def getRankMap(mxSet, tlsaSet):
    f = open(alexa_path, 'r')
    
    rankMap = {}
    reader = csv.reader(f)
    for line in reader:
        if len(line) < 2:
            print("Wrong input")
            print(line)
            input()
        rank = int(line[0])
        if rank in rankMap:
            print("Wrong rank")
            print(line)
            input()
        rankMap[rank] = [False, False]
        dn = line[1] +"."
        if dn in mxSet:
            rankMap[rank][0] = True
        if dn in tlsaSet:
            rankMap[rank][1] = True
    f.close()

    return rankMap

def getStat(rankMap):

    f = open(output_path, "w")
    f.write("#bin, num of domains with MX, num of domains with MX and TLSA\n")

    ranks = list(rankMap.keys())
    ranks.sort()

    mybin = 10000
    mxNum = 0
    tlsaNum = 0
    for rank in ranks:
        if not rankMap[rank][0] and rankMap[rank][1]:
            print(rank)
            input()

        if rankMap[rank][0]:
            mxNum += 1
        if rankMap[rank][0] and rankMap[rank][1]:
            tlsaNum += 1
        if rank % 10000 == 0:
            f.write(str(int(rank/10000-1)) + "," + str(mxNum) + "," + str(tlsaNum) + "\n")
            mxNum = 0
            tlsaNum = 0
    f.close()

if __name__ == "__main__":
    mxSet = getMX()
    tlsaSet = getTLSA()

    rankMap = getRankMap(mxSet, tlsaSet)

    getStat(rankMap)


