#!/user/bin/python3
from pyspark import SparkContext

import sys, os
import dns as mydns
import dns.message as mymessage
import base64
from OpenSSL import crypto
import json
import datetime
import hashlib

target_dn_path = "" # path to rollover-candidate-[city].txt which is the output of rollover-candidate-sub.py, ex) /home/ubuntu/output/rollover-candidate-virginia.txt
dependency_path = "" # path to 'dependencies.zip'
input_path = "" # ex) /user/hadoop/data/virginia
output_path = "" # ex) /user/hadoop/rollover_output_virginia/


cert_path = "/etc/ssl/certs/"
keyChangedSet = []
root_certs = {}

def tojson(line):
    return json.loads(line)

def toCSV(line):
    return " ".join(str(d) for d in line)

def toTime(x):
    return datetime.datetime.strptime(x, '%Y%m%d %H')

def matchCrt(matching, crt, adata):
    from OpenSSL import crypto

    crt = base64.b64decode(crt)
    crt = crypto.load_certificate(crypto.FILETYPE_PEM, crt)
    crt = crypto.dump_certificate(crypto.FILETYPE_ASN1, crt)

    if matching == '0':
        tmpCert = crypto.load_certificate(crypto.FILETYPE_ASN1, crt)
        hashed = crypto.dump_certificate(crypto.FILETYPE_PEM, tmpCert).lower().decode()
        hashed = hashed.replace('-----begin certificate-----', '').replace('-----end certificate-----', '').replace('\n', '')
        adata = adata.lower().decode().replace('-----begin certificate-----', '').replace('-----end certificate-----', '').replace('\n', '')
    elif matching == '1':
        hashed = hashlib.sha256(crt).hexdigest()
    elif matching == '2':
        hashed = hashlib.sha512(crt).hexdigest()
    else:
        return False, 'wrongMatchingType'

    if hashed == adata:
        return True, None
    return False, None

def matchKey(matching, crt, adata):
    from OpenSSL import crypto
    
    crt = base64.b64decode(crt)
    crt = crypto.load_certificate(crypto.FILETYPE_PEM, crt)
    key = crt.get_pubkey()
    key = crypto.dump_publickey(crypto.FILETYPE_ASN1, key)

    if matching == '0':
        hashed = crypto.dump_publickey(crypto.FILETYPE_PEM, key).lower().decode()
        hashed = hashed.replace('-----begin public key-----', '').replace('-----end public key-----', '').replace('\n', '')
        adata = adata.lower().decode().replace('-----begin public key-----', '').replace('-----end public key-----', '').replace('\n', '')
    elif matching == '1':
        hashed = hashlib.sha256(key).hexdigest()
    elif matching == '2':
        hashed = hashlib.sha512(key).hexdigest()
    else:
        return False, 'wrongMatchingType'

    if hashed == adata:
        return True, None
    return False, None

def parseTLSA(encoded):
    msg = base64.b64decode(encoded)
    msg = mymessage.from_wire(msg)
    rrset = []

    for answer in msg.answer:
        if answer.rdtype == 52:
            rrset = rrset + [data.to_text() for data in answer]
    return rrset

def getTTL(encoded):
    msg = base64.b64decode(encoded)
    msg = mymessage.from_wire(msg)

    for answer in msg.answer:
        if answer.rdtype == 52:
            ttl = answer.to_rdataset().ttl
    return ttl

def getNearTTL(dataList, time):

    prev = None
    noPast = False
    for data in dataList:
        if 'tlsa' in data:
            prev = data
        if time == data['time']:
            break
    if not prev == None:
        return getTTL(prev['tlsa']['record_raw'])
    else:
        flag = False
        for data in dataList:
            if 'tlsa' in data:
                if flag:
                    return getTTL(data['tlsa']['record_raw'])
            if time == data['time']:
                flag = True
        return None

def getNearTLSA(dataList, time):

    prev = None
    for data in dataList:
        if 'tlsa' in data:
            prev = data['tlsa']['record_raw']
        if data['time'] == time:
            break

    return prev

def getUsage(records):

    return set([x.split()[0] for x in records])

def usageCase(usages):
    case = None
    if len(usages) == 1:
        case = 0 # EE or TA only usages
    elif len(usages) == 2:
        if '1' in usages and '3' in usages:
            case = 0
        elif '0' in usages and '2' in usages:
            case = 0
        else:
            case = 1 # EE and TA mixed usages
    else:
        case = 1

    return case

def getTLSA(d):
    if 'tlsa' in d:
        return parseTLSA(d['tlsa']['record_raw'])
    return None
    
def getCert(d):
    if 'certs' in d['starttls']:
        return d['starttls']['certs']
    return None

def getKey(d):
    from OpenSSL import crypto
    certs = getCert(d)
    if certs == None:
        return None

    keys = []
    for cert in certs:
        crt = base64.b64decode(cert)
        crt = crypto.load_certificate(crypto.FILETYPE_PEM, crt)
        key = crt.get_pubkey()
        key = crypto.dump_publickey(crypto.FILETYPE_ASN1, key)
        key = base64.b64encode(key)
        keys.append(key)
    return keys
  
def diffKeyLocation(prev, curr):
    index = 0
    if len(prev) == 1 and len(curr) == 1:
        index = 1
    elif len(prev) == 1 or len(curr) == 1:
        if prev[0] == curr[0]:
            index = 2
        else:
            index = 0
    else:
        if not prev[0] == curr[0]:
            if not set(prev[1:]) == set(curr[1:]):
                index = 0 # leaf, TA changed
            else:
                index = 1 # leaf changed
        else:
            if not set(prev[1:]) == set(curr[1:]):
                index = 2 # TA changed
            else:
                index = 3 # leaf, TA both are not changed
    return index

def getChangedTimes(x):
    
    timeline = []
    prevCert = None
    prevKey = None
    for data in x:
        currKey = getKey(data)
        currCert = getCert(data)
        if prevKey == None:
            prevCert = currCert
            prevKey = currKey

        if currKey == None:
            continue

        if not set(prevKey) == set(currKey):
            if not currKey == None:
                index = diffKeyLocation(prevKey, currKey)
                if not index == 3:
                    timeline.append((data['time'], data, prevCert, index))
                prevCert = currCert
                prevKey = currKey

    return timeline
                    
def getThresTime(dataList, time, ttl):
    currTime = datetime.datetime.strptime(time, "%Y%m%d %H")
    thresTime = currTime - datetime.timedelta(seconds=(2*ttl))
    thresTime = thresTime.strftime("%Y%m%d %H")

    thresTime = thresTime.split()
    hour = thresTime[1]
    if hour[0] == '0':
        thresTime[1] = hour[1]

    thresTime = ' '.join(thresTime)

    for data in dataList:
        if data['time'] == thresTime:
            return data, thresTime
    return None, thresTime

def findNearThresTime(dataList, thresTime):

    prev = None
    for data in dataList:
        curr = datetime.datetime.strptime(data['time'], "%Y%m%d %H")
        thres = datetime.datetime.strptime(thresTime, "%Y%m%d %H")
        if curr >= thres:
            break    
        if 'tlsa' in data:
            prev = data

    if not prev == None:
        return parseTLSA(prev['tlsa']['record_raw'])
    else:
        return None

def findRoot(cert):
    from OpenSSL import crypto
    crt = base64.b64decode(cert)
    crt = crypto.load_certificate(crypto.FILETYPE_PEM, crt)
    issuer = crt.get_issuer()

    if issuer == None:
        return None

    roots = set(root_certs.keys())

    if issuer.CN in roots:
        root = base64.b64encode(root_certs[issuer.CN].encode())
        return root
    return None

def isRoot(cert):
    from OpenSSL import crypto
    crt = base64.b64decode(cert)
    crt = crypto.load_certificate(crypto.FILETYPE_PEM, crt)
    issuer = crt.get_issuer()
    subject = crt.get_subject()

    if issuer == None or subject == None:
        return False

    if issuer.CN == subject.CN:
        return True
    return False

def unitValidation(record, certs, index):
    from OpenSSL import crypto

    record = record.split()
    if len(record) != 4:
        record = record[:3] + [''.join(record[3:])]
    usage = record[0]
    selector = record[1]
    matching = record[2]
    data = record[3]

    matched = False
    error = None
    if index == 0:
        # Check leaf, TA cert both
        if usage == '1' or usage == '3':
            if selector == '0':
                matched, error = matchCrt(matching, certs[0], data)
            elif selector == '1':
                matched, error = matchKey(matching, certs[0], data)
            else:
                return False, 'SelectorError'
            return matched, error
        elif usage == '0':
            if isRoot(certs[-1]):
                tmpCerts = certs
            else:
                root = findRoot(certs[-1])
                if root == None:
                    tmpCerts = certs
                else:
                    tmpCerts = certs + [root]
            for cert in tmpCerts[1:]:
                if selector == '0':
                    matched, error = matchCrt(matching, cert, data)
                elif selector == '1':
                    matched, error = matchKey(matching, cert, data)
                else:
                    return False, 'SelectorError'
                if matched:
                    return matched, None
            return matched, error
        elif usage == '2':
            # Check TA cert
            if selector == '0':
                matched, error = matchCrt(matching, certs[-1], data)
                return matched, error
            elif selector == '1':
                matched, error = matchKey(matching, certs[-1], data)
                return matched, error
            else:
                return False, 'SelectorError'
            return matched, error 
        else:
            return False, 'UsageError'

        return False, error
    elif index == 1:
        # Check only leaf cert
        if usage == '1' or usage == '3':
            if selector == '0':
                matched, error = matchCrt(matching, certs[0], data)
                return matched, error
            elif selector == '1':
                matched, error = matchKey(matching, certs[0], data)
                return matched, error
            else:
                return False, 'SelectorError'
        elif usage == '0' or usage == '2':
            return None, "NotRollover"
        else:
            return False, 'UsageError'
    elif index == 2:
        # Check only TA cert
        if usage == '0':
            if isRoot(certs[-1]):
                tmpCerts = certs
            else:
                root = findRoot(certs[-1])
                if root == None:
                    tmpCerts = certs
                else:
                    tmpCerts = certs + [root]
            for cert in tmpCerts[1:]:
                if selector == '0':
                    matched, error = matchCrt(matching, cert, data)
                elif selector == '1':
                    matched, error = matchKey(matching, cert, data)
                else:
                    return False, 'SelectorError'
                if matched:
                    return matched, None
            return matched, error

        elif usage == '2':
            if selector == '0':
                matched, error = matchCrt(matching, certs[-1], data)
            elif selector == '1':
                matched, error = matchKey(matching, certs[-1], data)
            else:
                return False, 'SelectorError'
            return matched, error
        elif usage == '1' or usage == '3':
            return None, 'NotRollover'
        else:
            return False, 'UsageError'
        return matched, error
    else:
        # Maybe do not enter here
        return False, 'WrongIndex?'


def validation(tlsaRecords, certs, index):
   
    results = []
    #tlsaRecords = parseTLSA(tlsa)
    for record in tlsaRecords:
        valid, error = unitValidation(record, certs, index)
        if valid:
            return True, None #, hashed, data
        else:
            results.append(valid)

    if False in results:
        return False, error
    else:
        return None, error

def isValidAfter(dataList, startTime, isRightAfter, certs, index):

    startTime = datetime.datetime.strptime(startTime, "%Y%m%d %H")
    for data in dataList:
        currTime = datetime.datetime.strptime(data['time'], "%Y%m%d %H")

        if startTime >= currTime:
            continue
        if not 'tlsa' in data:
            continue

        valid, error = validation(parseTLSA(data['tlsa']['record_raw']), certs, index)

        if isRightAfter:
            if valid == None:
                return None, None, None
            elif valid:
                return True, data['time'], data
            else:
                return False, data['time'], data
        else:
            if valid:
                return True, data['time'], data

    return False, None, None

def findAfterTLSA(dataList, time):
    after = None
    changedTime = datetime.datetime.strptime(time, "%Y%m%d %H")
    targetTime = changedTime + datetime.timedelta(hours=1)
    targetTime = targetTime.strftime("%Y%m%d %H")
    for data in dataList:
        if data['time'] < targetTime:
            continue

        if 'tlsa' in data:
            after = data

        if data['time'] >= targetTime and not after == None:
            break
    if after == None:
        return None

    if after['time'] == targetTime:
        return parseTLSA(after['tlsa']['record_raw'])
    else:
        prev = None
        for data in dataList:
            if data['time'] >= targetTime:
                break
            if 'tlsa' in data:
                prev = data

        if prev == None:
            return None
        prevTLSA = parseTLSA(prev['tlsa']['record_raw'])
        afterTLSA = parseTLSA(prev['tlsa']['record_raw'])

        if set(prevTLSA) == set(afterTLSA):
            return afterTLSA
        else:
            return None

def findPrevTLSA(dataList, time):
    prev = None
    changedTime = datetime.datetime.strptime(time, "%Y%m%d %H")
    targetTime = changedTime - datetime.timedelta(hours=1)
    targetTime = targetTime.strftime("%Y%m%d %H")
    for data in dataList:
        if data['time'] > targetTime:
            break

        if 'tlsa' in data:
            prev = data

    if prev == None:
        return None
    
    if prev['time'] == targetTime:
        return parseTLSA(prev['tlsa']['record_raw'])
    else:
        after = None
        for data in dataList:
            if data['time'] <= targetTime:
                continue
            if 'tlsa' in data:
                after = data
                break
        if after == None:
            return None

        prevTLSA = parseTLSA(prev['tlsa']['record_raw'])
        afterTLSA = parseTLSA(after['tlsa']['record_raw'])

        if set(prevTLSA) == set(afterTLSA):
            return prevTLSA
        else:
            return None

def findPrevTLSA(dataList, time):
    prev = None
    changedTime = datetime.datetime.strptime(time, "%Y%m%d %H")
    targetTime = changedTime - datetime.timedelta(hours=1)
    targetTime = targetTime.strftime("%Y%m%d %H")
    for data in dataList:
        if data['time'] > targetTime:
            break

        if 'tlsa' in data:
            prev = data

    if prev == None:
        return None
    
    if prev['time'] == targetTime:
        return parseTLSA(prev['tlsa']['record_raw'])
    else:
        after = None
        for data in dataList:
            if data['time'] <= targetTime:
                continue
            if 'tlsa' in data:
                after = data
                break
        if after == None:
            return None

        prevTLSA = parseTLSA(prev['tlsa']['record_raw'])
        afterTLSA = parseTLSA(after['tlsa']['record_raw'])

        if set(prevTLSA) == set(afterTLSA):
            return prevTLSA
        else:
            return None


def findMatching(tlsa, certs):
    matched = []
    for record in tlsa:
        isMatched, error = unitValidation(record, certs, 0)
        if isMatched:
            matched.append(record)
    return matched

def case0Check(dataList, timeData, ttl):
    def timeFormat(t):
        tt = t.replace(" ", ":")
        return tt

    time = timeData[0]
    data = timeData[1]
 
    needThres = False
    thresTimeData, thresTime = getThresTime(dataList, time, ttl)
    if thresTimeData == None or 'tlsa' not in thresTimeData:
        needThres = True

    if needThres:
        thresTimeTLSA = findNearThresTime(dataList, thresTime)
        if thresTimeTLSA == None:
            result = "CannotKnowPreviousTLSA"
            return result
    else:
        thresTimeTLSA = parseTLSA(thresTimeData['tlsa']['record_raw'])

    prevCerts = timeData[2]
    newCerts = data['starttls']['certs']
    index = timeData[3]
       
    # check the new key
    newKeyValid, error = validation(thresTimeTLSA, newCerts, index)

    # check the old key
    oldKeyValid, error = validation(thresTimeTLSA, prevCerts, index)

    if needThres:
        if newKeyValid == None and oldKeyValid == None:
            result = "NotRollover"
        elif newKeyValid == None or oldKeyValid == None:
            result = "SomethingWrong1"
        elif newKeyValid and oldKeyValid:
            # Check the old key is valid afterward
            isValid, when, tmpD = isValidAfter(dataList, thresTime, True, prevCerts, index)
            if isValid:
                result ="BothValid"
            else:
                result = "NewKeyValid/OldKeyUnknown"
        elif newKeyValid and not oldKeyValid:
            result = "OldKeyInvalid"
        elif not newKeyValid and oldKeyValid:
            # OldKey is valid
            # Need to check the new key and old key is valid afterward
            isValidNew, whenNew, tmpDNew = isValidAfter(dataList, thresTime, True, newCerts, index)
            isValidOld, whenOld, tmpDOld = isValidAfter(dataList, thresTime, True, prevCerts, index)
           
            if isValidOld == None:
                result  = "CannotKnow" 
            elif isValidNew == None:
                result = "NotRollover"
            elif isValidNew:
                # OldKey is valid / NewKey cannot know
                result = "CannotKnow"
            elif isValidNew and not isValidOld:
                # OldKey, NewKey cannot know
                result = "CannotKnow"
            elif not isValidNew and isValidOld:
                # OldKey is valid and NewKey is invalid
                isValidNew2, whenNew2, tmpDNew2 = isValidAfter(dataList, thresTime, False, newCerts, index)
                if isValidNew2 == None:
                    whenNew2 = "WRONG!!"
                if not isValidNew2:
                    whenNew2 = "None"
                result = "NewKeyInvalid-" + timeFormat(time) + "-ThresTime-" + timeFormat(thresTime) + "-ValidAfter-" + timeFormat(whenNew2)
            else:
                # OldKey cannot know and NewKey is invalid
                isValidNew2, whenNew2, tmpDNew2 = isValidAfter(dataList, thresTime, False, newCerts, index)
                if isValidNew2 == None:
                    whenNew2 = "WRONG!!"
                if not isValidNew2:
                    whenNew2 = "None"
                result = "NewKeyInvalid/OldKeyUnknown-" + timeFormat(time) + "-ThresTime-" + timeFormat(thresTime) + "-ValidAfter-" + timeFormat(whenNew2)

        else:
            # OldKey is not valid
            # Need to check the new key is valid afterward
            isValid, when, tmp = isValidAfter(dataList, thresTime, True, newCerts, index)
            if isValid == None:
                result = "OldKeyInvalid"
            elif isValid:
                result = "NewKeyUnknown/OldKeyInvalid"
            else:
                isValidNew2, whenNew2, tmpDNew2 = isValidAfter(dataList, thresTime, False, newCerts, index)
                if isValidNew2 == None:
                    whenNew2 = "WRONG!!"
                if not isValidNew2:
                    whenNew2 = "None"
                result = "BothInvalid-" + timeFormat(time) + "-ThresTime-" + timeFormat(thresTime) + "-ValidAfter-" + timeFormat(whenNew2)
    else:
        if newKeyValid == None and oldKeyValid == None:
            result = "NotRollover"
        elif newKeyValid == None or oldKeyValid == None:
            result = "SomethingWrong2"
        elif newKeyValid and oldKeyValid:
            result = "BothValid"
        elif newKeyValid and not oldKeyValid:
            result = "OldKeyInvalid"
        elif not newKeyValid and oldKeyValid:
            isValid, when, tmp = isValidAfter(dataList, thresTime, False, newCerts, index)
            if isValid == None:
                when = "WRONG!!"
            if not isValid:
                when = "None"
            result = "NewKeyInvalid-" + timeFormat(time) + "-ThresTime-" + timeFormat(thresTime) + "-ValidAfter-" + timeFormat(when)
        else:
            isValid, when, tmp = isValidAfter(dataList, thresTime, False, newCerts, index)
            if isValid == None:
                when = "WRONG!!"
            if not isValid:
                when = "None"
            result = "BothInvalid-" +timeFormat(time) + "-ThresTime-" + timeFormat(thresTime) + "-ValidAfter-" + timeFormat(when)
    return result


def rolloverCheck(x):
    dn = x[0]
    dataList = x[1]

    if dataList == None:
        return (dn, "NotTarget")

    changedTimes = getChangedTimes(dataList)

    results = []
    for timeData in changedTimes:
        time = timeData[0]
        data = timeData[1]
        
        if 'tlsa' not in data:
            ttl = getNearTTL(dataList, time)
            if ttl == None:
                results.append("AllTimeNoTLSA")
                continue
        else:
            ttl = getTTL(data['tlsa']['record_raw'])
            
        
        # check short TTL
        if 2*ttl < 3600:
            results.append("ShortTTL-"+str(ttl))
            continue

        prevTLSA = findPrevTLSA(dataList, time)
        if prevTLSA == None:
            results.append("CannotKnowPreviousTLSA")
            continue

        # get usages of right before 
        usages = getUsage(prevTLSA)

        case = usageCase(usages)
        caseStr = str(case) + "-"
        if case == 0:
            # EE or TA only usages
            result = case0Check(dataList, timeData, ttl)
            results.append(caseStr + result)
        else:
            # EE and TA mixed usages
            prevCerts = timeData[2]
            newCerts = data['starttls']['certs']
            index = timeData[3]
            matchingRecord = findMatching(prevTLSA, prevCerts)
            if matchingRecord == []:
                result = case0Check(dataList, timeData, ttl)
                results.append(caseStr + "0-" + result)
            else:
                if 'tlsa' in data:
                    currTLSA = parseTLSA(data['tlsa']['record_raw'])
                else:
                    currTLSA = findAfterTLSA(dataList, time)
                    if currTLSA == None:
                        result = ("CannotKnowAfterTLSA")
                        results.append(caseStr + result)
                        continue

                result = ""
                for record in matchingRecord:
                    if record in set(currTLSA):
                        isMatched, error = unitValidation(record, newCerts, 0)
                        if isMatched:
                            result = "WellRolloverOrNotRollover"
                            results.append(caseStr + result)
                            break
                if result == "":
                    result = case0Check(dataList, timeData, ttl)
                    results.append(caseStr + "0-" + result)

    return [dn] + results


def genList(data):
    dataList = [t for t in data[1]]
    dataList = sorted(dataList, key=lambda e: toTime(e['time']))

    if not data[0] in keyChangedSet:
        return (data[0], None)

    return (data[0], dataList)


def initKeyChangedSet():
    global keyChangedSet
    keyChangedSet = []

def readKeyChangedSet():
    global keyChangedSet
   
    initKeyChangedSet()
    f = open(target_dn_path, "r")
    while True:
        line = f.readline()
        if not line: break
        line = line[:-1]

        keyChangedSet.append(line)

    f.close()

def getRootCerts():
    global root_certs

    files = os.listdir(cert_path)
    files.remove("java")

    for filename in files:
        f = open(os.path.join(cert_path, filename), "r")
        cert = f.read()
        crt = crypto.load_certificate(crypto.FILETYPE_PEM, cert)
        issuer = crt.get_issuer().CN

        root_certs[issuer] = cert
        f.close()

def getStats(sc):
    def dnGroup(x):
        return x['domain'] + ":" + x['port']

    getRootCerts()

    readKeyChangedSet()


    k = sc.textFile(input_path)\
            .map(tojson)\
            .groupBy(dnGroup)\
            .map(genList)\
            .map(rolloverCheck)\
            .map(toCSV)

    k.saveAsTextFile(output_path)

if __name__ == "__main__":
    sc = SparkContext(appName="DANE-ROLLOVER-TEST")
    sc.addPyFile(dependency_path)
    getStats(sc)
    sc.stop()
