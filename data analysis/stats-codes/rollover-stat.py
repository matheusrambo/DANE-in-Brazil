import os
import datetime

input_path = "" # path to result of rollover.py, ex. /home/ubuntu/rollover_output_virginia/ 
output_path = "" # path to output


if __name__ == "__main__":
    files = os.listdir(input_path)

    usage0 = 0
    usage1 = 0

    total = 0
    notTarget = 0
    ShortTTL = 0
    CannotKnow = 0
    NotRollover = 0
    AllTimeGood = 0
    AllTimeGood_0 = 0
    AllTimeGood_1 = 0
    Bad = 0
    Bad_0 = 0
    Bad_1 = 0

    NewKeyInvalid = 0
    NewKeyInvalid_0 = 0
    NewKeyInvalid_1 = 0
    OldKeyInvalid = 0
    OldKeyInvalid_0 = 0
    OldKeyInvalid_1 = 0
    BothKeyInvalid = 0
    BothKeyInvalid_0 = 0
    BothKeyInvalid_1 = 0
    NewKeyInvalidOldKeyUnknown = 0
    NewKeyInvalidOldKeyUnknown_0 = 0
    NewKeyInvalidOldKeyUnknown_1 = 0
    NewKeyUnknownOldKeyInvalid = 0
    NewKeyUnknownOldKeyInvalid_0 = 0
    NewKeyUnknownOldKeyInvalid_1 = 0

    NewKeyInvalidOldKeyUnknown_validSame = 0
    NewKeyInvalidOldKeyUnknown_validBtw = 0
    NewKeyInvalidOldKeyUnknown_validAfter = 0
    NewKeyInvalidOldKeyUnknown_validNone = 0
    
    NewKeyInvalid_validSame = 0
    NewKeyInvalid_validBtw = 0
    NewKeyInvalid_validAfter = 0
    NewKeyInvalid_validNone = 0

    NewKeyInvalidOldKeyUnknown_validSame_0 = 0
    NewKeyInvalidOldKeyUnknown_validBtw_0 = 0
    NewKeyInvalidOldKeyUnknown_validAfter_0 = 0
    NewKeyInvalidOldKeyUnknown_validNone_0 = 0
    
    NewKeyInvalid_validSame_0 = 0
    NewKeyInvalid_validBtw_0 = 0
    NewKeyInvalid_validAfter_0 = 0
    NewKeyInvalid_validNone_0 = 0

    NewKeyInvalidOldKeyUnknown_validSame_1 =0
    NewKeyInvalidOldKeyUnknown_validBtw_1 = 0
    NewKeyInvalidOldKeyUnknown_validAfter_1 = 0
    NewKeyInvalidOldKeyUnknown_validNone_1 = 0
    
    NewKeyInvalid_validSame_1 = 0
    NewKeyInvalid_validBtw_1 = 0
    NewKeyInvalid_validAfter_1 = 0
    NewKeyInvalid_validNone_1 = 0

    invalid_domains = set([])
    mixed_usage = []

    for filename in files:
        f = open(os.path.join(input_path, filename), "r")
        while True:
            line = f.readline()
            if not line: break

            line = line.split()
            dn = line[0]
            results = line[1:]
            total += 1

            if results[0] == "NotTarget":
                notTarget += 1
                continue
           
            usage0_tmp = 0
            usage1_tmp = 0
            AllTimeNoTLSA_tmp = 0
            ShortTTL_tmp = 0
            CannotKnowPreviousTLSA_tmp = 0
            CannotKnowAfterTLSA_tmp = 0
            WellRolloverOrNotRollover_tmp = 0
            NotRollover_tmp = 0
            SomethingWrong = 0
            BothValid_tmp = 0
            BothInvalid_tmp = 0
            NewKeyValidOldKeyUnknown_tmp = 0
            OldKeyInvalid_tmp = 0
            CannotKnow_tmp = 0
            NewKeyInvalid_tmp = 0
            NewKeyInvalidOldKeyUnknown_tmp = 0
            NewKeyUnknownOldKeyInvalid_tmp = 0
            NewKeyInvalid_tmp = 0

            NewKeyInvalidOldKeyUnknown_validSame_tmp = 0
            NewKeyInvalidOldKeyUnknown_validBtw_tmp = 0
            NewKeyInvalidOldKeyUnknown_validAfter_tmp = 0
            NewKeyInvalidOldKeyUnknown_validNone_tmp = 0

            NewKeyInvalid_validSame_tmp = 0
            NewKeyInvalid_validBtw_tmp = 0
            NewKeyInvalid_validAfter_tmp = 0
            NewKeyInvalid_validNone_tmp = 0

            for result in results:
                if result == "AllTimeNoTLSA":
                    AllTimeNoTLSA_tmp += 1
                elif result == "CannotKnowPreviousTLSA":
                    CannotKnowPreviousTLSA_tmp += 1
                elif "ShortTTL" in result:
                    ShortTTL_tmp += 1

                elif result[:2] == "0-":
                    tmpResult = result[2:]
                    if tmpResult == "CannotKnowPreviousTLSA":
                        CannotKnowPreviousTLSA_tmp += 1
                        continue
                    if tmpResult == "NewKeyValid/OldKeyUnknown-A":
                        NewKeyValidOldKeyUnknown_tmp += 1
                        continue
                    if tmpResult == "NotRollover":
                        NotRollover_tmp += 1
                        continue
                    if tmpResult == "CannotKnow":
                        CannotKnow_tmp += 1
                        continue
                    if "BothInvalid" in tmpResult:
                        BothInvalid_tmp += 1
                        continue

                    usage0_tmp += 1
                    if "SomethingWrong" in tmpResult:
                        SomethingWrong += 1
                        print(line)
                    elif "BothValid" in tmpResult:
                        BothValid_tmp += 1
                    elif tmpResult == "OldKeyInvalid":
                        OldKeyInvalid_tmp += 1
                    elif "NewKeyInvalid/OldKeyUnknown" in tmpResult:
                        NewKeyInvalidOldKeyUnknown_tmp += 1
                        timeSplit = tmpResult.split("-")
                        keyChanged = timeSplit[1]
                        thres = timeSplit[3]
                        valid = timeSplit[5]

                        if valid == "None":
                            NewKeyInvalidOldKeyUnknown_validNone_tmp += 1
                        else:
                            keyChangedTime = datetime.datetime.strptime(keyChanged, "%Y%m%d:%H")
                            thresTime = datetime.datetime.strptime(thres, "%Y%m%d:%H")
                            validTime = datetime.datetime.strptime(valid, "%Y%m%d:%H")

                            if validTime < keyChangedTime:
                                NewKeyInvalidOldKeyUnknown_validBtw_tmp += 1
                            elif validTime == keyChangedTime:
                                NewKeyInvalidOldKeyUnknown_validSame_tmp += 1
                            else:
                                NewKeyInvalidOldKeyUnknown_validAfter_tmp+= 1
                    elif tmpResult == "NewKeyUnknown/OldKeyInvalid":
                        NewKeyUnknownOldKeyInvalid_tmp += 1
                    elif "NewKeyInvalid" in tmpResult:
                        NewKeyInvalid_tmp += 1
                        timeSplit = tmpResult.split("-")
                        keyChanged = timeSplit[1]
                        thres = timeSplit[3]
                        valid = timeSplit[5]

                        if valid == "None":
                            NewKeyInvalid_validNone_tmp += 1
                        else:
                            keyChangedTime = datetime.datetime.strptime(keyChanged, "%Y%m%d:%H")
                            thresTime = datetime.datetime.strptime(thres, "%Y%m%d:%H")
                            validTime = datetime.datetime.strptime(valid, "%Y%m%d:%H")

                            if validTime < keyChangedTime:
                                NewKeyInvalid_validBtw_tmp += 1
                            elif validTime == keyChangedTime:
                                NewKeyInvalid_validSame_tmp += 1
                            else:
                                NewKeyInvalid_validAfter_tmp+= 1
                    else:
                        print(line)

                elif result[:4] == "1-0-":
                    tmpResult = result[4:]
                    if tmpResult == "CannotKnowPreviousTLSA":
                        CannotKnowPreviousTLSA_tmp += 1
                        continue
                    if tmpResult == "NewKeyValid/OldKeyUnknown":
                        NewKeyValidOldKeyUnknown_tmp += 1
                        continue
                    if tmpResult == "NotRollover":
                        NotRollover_tmp += 1
                        continue
                    if tmpResult == "CannotKnow":
                        CannotKnow_tmp += 1
                        continue
                    if "BothInvalid" in tmpResult:
                        BothInvalid_tmp += 1
                        continue

                    usage1_tmp += 1
                    if "SomethingWrong" in tmpResult:
                        SomethingWrong += 1
                    elif "BothValid" in tmpResult:
                        BothValid_tmp += 1
                    elif tmpResult == "OldKeyInvalid":
                        OldKeyInvalid_tmp += 1
                    elif "NewKeyInvalid/OldKeyUnknown" in tmpResult:
                        NewKeyInvalidOldKeyUnknown_tmp += 1
                        keyChanged = timeSplit[1]
                        thres = timeSplit[3]
                        valid = timeSplit[5]

                        if valid == "None":
                            NewKeyInvalidOldKeyUnknown_validNone_tmp += 1
                        else:
                            keyChangedTime = datetime.datetime.strptime(keyChanged, "%Y%m%d:%H")
                            thresTime = datetime.datetime.strptime(thres, "%Y%m%d:%H")
                            validTime = datetime.datetime.strptime(valid, "%Y%m%d:%H")

                            if validTime < keyChangedTime:
                                NewKeyInvalidOldKeyUnknown_validBtw_tmp += 1
                            elif validTime == keyChangedTime:
                                NewKeyInvalidOldKeyUnknown_validSame_tmp += 1
                            else:
                                NewKeyInvalidOldKeyUnknown_validAfter_tmp+= 1
 
                    elif tmpResult == "NewKeyUnknown/OldKeyInvalid":
                        NewKeyUnknownOldKeyInvalid_tmp += 1
                    elif "NewKeyInvalid" in tmpResult:
                        NewKeyInvalid_tmp += 1
                        keyChanged = timeSplit[1]
                        thres = timeSplit[3]
                        valid = timeSplit[5]

                        if valid == "None":
                            NewKeyInvalid_validNone_tmp += 1
                        else:
                            keyChangedTime = datetime.datetime.strptime(keyChanged, "%Y%m%d:%H")
                            thresTime = datetime.datetime.strptime(thres, "%Y%m%d:%H")
                            validTime = datetime.datetime.strptime(valid, "%Y%m%d:%H")

                            if validTime < keyChangedTime:
                                NewKeyInvalid_validBtw_tmp += 1
                            elif validTime == keyChangedTime:
                                NewKeyInvalid_validSame_tmp += 1
                            else:
                                NewKeyInvalid_validAfter_tmp+= 1
 
                    else:
                        print(line)

                elif result[:2] == "1-":
                    tmpResult = result[2:]
                    if tmpResult == "CannotKnowAfterTLSA":
                        CannotKnowAfterTLSA_tmp += 1
                        continue
                    usage1_tmp += 1
                    if tmpResult == "WellRolloverOrNotRollover":
                        WellRolloverOrNotRollover_tmp += 1
                    else:
                        print(line)
            
            if usage0_tmp > 0 and usage1_tmp > 0:
                mixed_usage.append(line)
                continue

            if NewKeyInvalidOldKeyUnknown_validBtw_tmp > 0:
                NewKeyInvalidOlkKeyUnknown_validBtw += 1
                if usage0_tmp > 0:
                    NewKeyInvalidOldKeyUnknown_validBtw_0 += 1
                if usage1_tmp > 0:
                    NewKeyInvalidOldKeyUnknown_validBtw_1 += 1

            if NewKeyInvalidOldKeyUnknown_validSame_tmp > 0:
                NewKeyInvalidOlkKeyUnknown_validSame += 1
                if usage0_tmp > 0:
                    NewKeyInvalidOldKeyUnknown_validSame_0 += 1
                if usage1_tmp > 0:
                    NewKeyInvalidOldKeyUnknown_validSame_1 += 1

            if NewKeyInvalidOldKeyUnknown_validAfter_tmp > 0:
                NewKeyInvalidOlkKeyUnknown_validAfter += 1
                if usage0_tmp > 0:
                    NewKeyInvalidOldKeyUnknown_validAfter_0 += 1
                if usage1_tmp > 0:
                    NewKeyInvalidOldKeyUnknown_validAfter_1 += 1

            if NewKeyInvalidOldKeyUnknown_validNone_tmp > 0:
                NewKeyInvalidOldKeyUnknown_validNone += 1
                if usage0_tmp > 0:
                    NewKeyInvalidOldKeyUnknown_validNone_0 += 1
                if usage1_tmp > 0:
                    NewKeyInvalidOldKeyUnknown_validNone_1 += 1

            if NewKeyInvalid_validBtw_tmp > 0:
                NewKeyInvalid_validBtw += 1
                if usage0_tmp > 0:
                    NewKeyInvalid_validBtw_0 += 1
                if usage1_tmp > 0:
                    NewKeyInvalid_validBtw_1 += 1

            if NewKeyInvalid_validAfter_tmp > 0:
                NewKeyInvalid_validAfter+= 1
                if usage0_tmp > 0:
                    NewKeyInvalid_validAfter_0 += 1
                    invalid_domains.add(dn)
                if usage1_tmp > 0:
                    NewKeyInvalid_validAfter_1 += 1

            if NewKeyInvalid_validSame_tmp > 0:
                NewKeyInvalid_validSame += 1
                if usage0_tmp > 0:
                    NewKeyInvalid_validSame_0 += 1
                    invalid_domains.add(dn)
                if usage1_tmp > 0:
                    NewKeyInvalid_validSame_1 += 1

            if NewKeyInvalid_validNone_tmp > 0:
                NewKeyInvalid_validNone += 1
                if usage0_tmp > 0:
                    NewKeyInvalid_validNone_0 += 1
                if usage1_tmp > 0:
                    NewKeyInvalid_validNone_1 += 1


            if NewKeyInvalid_tmp > 0:
                NewKeyInvalid += 1
                if usage0_tmp > 0:
                    NewKeyInvalid_0 += 1
                if usage1_tmp > 0:
                    NewKeyInvalid_1 += 1
            if OldKeyInvalid_tmp > 0:
                OldKeyInvalid += 1
                if usage0_tmp > 0:
                    OldKeyInvalid_0 += 1
                if usage1_tmp > 0:
                    OldKeyInvalid_1 += 1
            if NewKeyInvalidOldKeyUnknown_tmp > 0:
                NewKeyInvalidOldKeyUnknown += 1
                if usage0_tmp > 0:
                    NewKeyInvalidOldKeyUnknown_0 += 1
                if usage1_tmp > 0:
                    NewKeyInvalidOldKeyUnknown_1 += 1
            if NewKeyUnknownOldKeyInvalid_tmp > 0:
                NewKeyUnknownOldKeyInvalid += 1
                if usage0_tmp > 0:
                    NewKeyUnknownOldKeyInvalid_0 += 1
                if usage1_tmp > 0:
                    NewKeyUnknownOldKeyInvalid_1 += 1

           
            tmpResult = []
            for result in results:
                if "BothInvalid" in result:
                    tmpResult.append(result[:13])
                elif "NewKeyInvalid" in result:
                    tmpResult.append(result[:15])
                else:
                    tmpResult.append(result)

            results = tmpResult

            if ShortTTL_tmp > 0:
                ShortTTL += 1
            elif OldKeyInvalid_tmp > 0 or NewKeyInvalid_tmp > 0 or NewKeyInvalidOldKeyUnknown_tmp > 0 or NewKeyUnknownOldKeyInvalid_tmp > 0 or NewKeyInvalid_tmp > 0:
                Bad += 1
                if usage0_tmp > 0:
                    Bad_0 += 1
                    usage0 += 1
                if usage1_tmp > 0:
                    Bad_1 += 1
                    usage1 += 1
            elif AllTimeNoTLSA_tmp > 0 or CannotKnowPreviousTLSA_tmp > 0 or CannotKnowAfterTLSA_tmp > 0 or NewKeyValidOldKeyUnknown_tmp > 0 or CannotKnow_tmp > 0:
                CannotKnow += 1
            else:
                if len(set(results)) == 1:
                    if BothValid_tmp > 0 or WellRolloverOrNotRollover_tmp > 0:
                        AllTimeGood += 1
                        if usage0_tmp > 0:
                            AllTimeGood_0 += 1
                            usage0 += 1
                        if usage1_tmp > 0:
                            AllTimeGood_1 += 1
                            usage1 += 1
                    elif NotRollover_tmp > 0:
                        NotRollover += 1
                    elif BothInvalid_tmp > 0:
                        BothKeyInvalid += 1
                    else:
                        print(line)
                elif len(set(results)) == 2:
                    if BothValid_tmp > 0 and WellRolloverOrNotRollover_tmp > 0:
                        AllTimeGood += 1
                        if usage0_tmp > 0:
                            AllTimeGood_0 += 1
                            usage0 += 1
                        if usage1_tmp > 0:
                            AllTimeGood_1 += 1
                            usage1 += 1
                    elif BothValid_tmp > 0 and NotRollover_tmp > 0:
                        AllTimeGood += 1
                        if usage0_tmp > 0:
                            AllTimeGood_0 += 1
                            usage0 += 1
                        if usage1_tmp > 0:
                            usgae1 += 1
                            AllTimeGood_1 += 1
                    elif WellRolloverOrNotRollover_tmp > 0 and NotRollover_tmp > 0:
                        AllTimeGood += 1
                        if usage0_tmp > 0:
                            AllTimeGood_0 += 1
                            usage0 += 1
                        if usage1_tmp > 0:
                            AllTimeGood_1 += 1
                            usgae1 += 1
                    elif BothValid_tmp > 0 and BothInvalid_tmp > 0:
                        AllTimeGood += 1
                        if usage0_tmp > 0:
                            AllTimeGood_0 += 1
                            usage0 += 1
                        if usage1_tmp > 0:
                            AllTimeGood_1 += 1
                            usgae1 += 1 
                    elif WellRolloverOrNotRollover_tmp > 0 and BothInvalid_tmp > 0:
                        AllTimeGood += 1
                        if usage0_tmp > 0:
                            AllTimeGood_0 += 1
                            usage0 += 1
                        if usage1_tmp > 0:
                            AllTimeGood_1 += 1
                            usgae1 += 1
                    elif NotRollover_tmp > 0 and BothInvalid_tmp > 0:
                        NotRollover += 1
                    else:
                        print(line)
                elif len(set(results)) == 3:
                    if BothValid_tmp > 0 and WellRolloverOrNotRollover_tmp > 0 and NotRollover_tmp > 0:
                        AllTimeGood += 1
                        if usage0_tmp > 0:
                            AllTimeGood_0 += 1
                            usage0 += 1
                        if usage1_tmp > 0:
                            AllTimeGood_1 += 1
                            usgae1 += 1
                else:
                    print(line)
 

        f.close()


for mixed in mixed_usage:
    flag = False
    for item in mixed[1:]:
        if "Invalid" in item:
            flag = True
    if flag:
        Bad += 1

f = open(output_path, "w")

target = total - notTarget
f.write("Total: " + str(total) + "\n")
f.write("Taget: " + str(target) + "\n")

f.write("BothKeyInvalid: " + str(BothKeyInvalid) + ", " + str(round(100*BothKeyInvalid/target, 2)) + "\n")
f.write("NotRollover: " + str(NotRollover) + ", " + str(round(100*NotRollover/target, 2)) +"\n")
f.write("ShortTTL: " + str(ShortTTL) + ", " + str(round(100*ShortTTL/target, 2)) + "\n")
f.write("CannotKnow: " + str( CannotKnow) + ", " + str(round(100*CannotKnow/target, 2)) +"\n")
f.write("\n")
f.write("AllTimeValid: " + str(AllTimeGood) + ", " + str(round(100*AllTimeGood/target, 2)) + "\n")
f.write("Invalid: "  + str(Bad) +", " + str(round(100*Bad/target, 2)) + "\n")
f.write("\n")
f.write("OnlyUsage: " + str(usage0))
f.write("AllTImeValid OnlyUsage: " + str(AllTimeGood_0) + ", " + str(round(100*AllTimeGood_0/usage0, 2)) + "\n")
f.write("Invalid OnlyUsage: " + str(Bad_0) + ", " + str(round(100*Bad_0/usage0, 2)) + "\n")
f.write("MixUsage: " + str(usage1))
f.write("AllTimeValid MixUsage: " + str(AllTimeGood_1) + ", " + str(round(100*AllTimeGood_1/usage1, 2)) +"\n")
f.write("Invalid MixUsage: " + str(Bad_1) + ", " + str(round(100*Bad_1/usage1, 2)) +"\n")

f.write("-------------------------------------\n")

f.write("NewKeyInvalid OnlyUsage: " + str(NewKeyInvalid_0) + ", " + str(round(100*NewKeyInvalid_0/Bad_0, 2)) +"\n")
f.write("OldKeyInvalid OnlyUsage: " + str(OldKeyInvalid_0) + ", " + str(round(100*OldKeyInvalid_0/Bad_0, 2)) +"\n")
f.write("NewKeyInvalidOldKeyUnknown OnlyUsage: " + str(NewKeyInvalidOldKeyUnknown_0) + ", " + str(round(100*NewKeyInvalidOldKeyUnknown_0/Bad_0, 2)) +"\n")
f.write("NewKeyUnknownOldKeyInvalid OnlyUsage: " + str(NewKeyUnknownOldKeyInvalid_0) + ", " + str(round(100*NewKeyUnknownOldKeyInvalid_0/Bad_0, 2)) + "\n")

f.write("NewKeyInvalid MixUsage: " + str(NewKeyInvalid_1) + ", " + str(round(100*NewKeyInvalid_1/Bad_1, 2)) + "\n");
f.write("OldKeyInvalid MixUsage: " + str(OldKeyInvalid_1) + ", " + str( round(100*OldKeyInvalid_1/Bad_1, 2)) + "\n")
f.write("NewKeyInvalidOldKeyUnknown MixUsage: " + str(NewKeyInvalidOldKeyUnknown_1) + ", " + str(round(100*NewKeyInvalidOldKeyUnknown_1/Bad_1, 2)) + "\n")
f.write("NewKeyUnknownOldKeyInvalid MixUsage: " + str(NewKeyUnknownOldKeyInvalid_1) + ", " + str(round(100*NewKeyUnknownOldKeyInvalid_1/Bad_1, 2)) + "\n")

f.write("-------------------------------------\n")

f.write("NewKeyInvalid Btw OnlyUsage: " + str(NewKeyInvalid_validBtw_0) + ", "+ str(round(100*NewKeyInvalid_validBtw_0/Bad_0, 2)) + "\n")
f.write("NewKeyInvalid Same OnlyUsage: " + str(NewKeyInvalid_validSame_0) + ", " + str(round(100*NewKeyInvalid_validSame_0/Bad_0, 2)) + "\n")
f.write("NewKeyInvalid After OnlyUsage: " + str(NewKeyInvalid_validAfter_0) + ", " + str(round(100*NewKeyInvalid_validAfter_0/Bad_0, 2)) + "\n")
f.write("NewKeyInvalid Not Changed OnlyUsage: " + str(NewKeyInvalid_validNone_0) + ", " + str(round(100*NewKeyInvalid_validNone_0/Bad_0, 2)) + "\n")

f.write("NewKeyInvalid Same + After OnlyUsage Domains: " + str(len(invalid_domains)) + ", " + str(round(100*len(invalid_domains)/Bad_0, 2)) + "\n")

f.write("NewKeyInvalid Btw MixUsage: " + str(NewKeyInvalid_validBtw_1) +", " + str(round(100*NewKeyInvalid_validBtw_1/Bad_1, 2)) + "\n")
f.write("NewKeyInvalid Same MixUsage: " + str(NewKeyInvalid_validSame_1) + ", " + str(round(100*NewKeyInvalid_validSame_1/Bad_1, 2)) +"\n")
f.write("NewKeyInvalid After MixUsage: " + str(NewKeyInvalid_validAfter_1) + ", " + str(round(100*NewKeyInvalid_validAfter_1/Bad_1, 2)) + "\n")
f.write("NewKeyInvalid Not Changed MixUsage: " + str(NewKeyInvalid_validNone_1) + ", " + str(round(100*NewKeyInvalid_validNone_1/Bad_1, 2)) + "\n")



f.write("\n")
f.write("Mixed_Usage(Need manual classification): " + str(mixed_usage) +"\n")
f.close()






