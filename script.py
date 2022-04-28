from dataclasses import dataclass
from importlib.abc import PathEntryFinder
from pathlib import Path
from hashlib import new
from re import T
from io import StringIO
import pandas as pd
import numpy as np
import os

def insertAtTop(path, line):
    mog = open(path, "r")
    contents = mog.readlines()
    contents.insert(0, line)
    mog = open(path, "w")
    contents = "".join(contents)
    mog.write(contents)

def parseFilename(filename):
    parser = filename.split('-')
    day = parser[3].split('_')
    parser.pop()
    parser.append(day[0])
    parser.append(day[1])
    print(len(parser))
    #Check Year & Month Validity 
    if not (parser[0] == "ppReport") & (len(parser)!= '5')):
        print("Error: The file format must be ppReport-Year-Month-Day_Hour")
        return (0)

    #Check Year Validity 
    if not (int(parser[1]) < 2022 & (1 <= int(parser[2])>= 12)):
        print("Error: The file format must be ppReport-Year-Month-Day_Hour")
        return (0)
        
    #Check Day & hour Validity
    if not (1 <= int(parser[3]) >= 31 & (0 <= int(parser[4])>= 23)):
        print("Error: The file format must be ppReport-Year-Month-Day_Hour")
        return (0)    
    

def updateLog (pathtolog, file):
    filelist = str(file).split(".")
    print(filelist)
    mog = open(pathtolog, "r+")
    if os.stat(pathtolog).st_size != 0:
        pas = mog.read().split("\n")
        pasa = [sent.split(",") for sent in pas if sent]
        i = int(pasa[0][0]) + 1     
    else:
        mog.write("0, "+ filelist[0] + '\n')
        mog.close()
        return("0", filelist[0])


    # if (pasa[0][1][1:]+".csv" != file):
    #     insertAtTop(pathtolog, (str(i)+ ", "+ filelist[0] + '\n'))
    # else: 
    #     print("Error: " + pasa[0][1][1:] + " Has already been inserted")
    #     exit(84)
    mog.close()
    return(str(i), filelist[0])

def setFile(file):
    data = pd.read_csv(file)
    
    # Check .log file for tracking ID and FILENAME
    fle = Path('.log')
    fle.touch(exist_ok=True)


    fileid, filename = updateLog(fle, file)
    data.insert(0, column="filename", value=filename) 
    data.insert(0, column="fileid", value=fileid) 

    parseFilename(filename)
    # print(data.columns)
    # print(data.head(10))
    return(data)

# Connecting to DB
def connectToBd(file):
    dataDf = setFile(file)

    # dataDf['fileid'] = dataDf['fileid'].astype(str)
    # dataDf['filename'] = dataDf['filename'].astype(str)
    # dataDf['msisdn'] = dataDf['msisdn'].astype(str)
    # dataDf['channel'] = dataDf['channel'].astype(str)
    # dataDf['channelcode'] = dataDf['channelcode'].astype(str)
    # dataDf['shortcode'] = dataDf['shortcode'].astype(str)
    # dataDf['requestid'] = dataDf['requestid'].astype(str)
    # dataDf['starttime'] = dataDf['starttime'].astype(str)
    # dataDf['endtime'] = dataDf['endtime'].astype(str)
    # dataDf['productid'] = dataDf['productid'].astype(str)
    # dataDf['productname'] = dataDf['productname'].astype(str)
    # dataDf['producttype'] = dataDf['producttype'].astype(str)
    # dataDf['productconstaint'] = dataDf['productconstaint'].astype(str)
    # dataDf['transactioncode'] = dataDf['transactioncode'].astype(str)
    # dataDf['refillid'] = dataDf['refillid'].astype(str)
    # dataDf['originoperatorid'] = dataDf['originoperatorid'].astype(str)
    # dataDf['correlationid'] = dataDf['correlationid'].astype(str)
    # dataDf['amountcharge'] = dataDf['amountcharge'].astype(str)
    # dataDf['momocharging'] = dataDf['momocharging'].astype(str)
    # dataDf['success/failure'] = dataDf['success/failure'].astype(str)
    # dataDf['reason'] = dataDf['reason'].astype(str)
    # dataDf['notification'] = dataDf['notification'].astype(str)
    # dataDf['sptype'] = dataDf['sptype'].astype(str)
    # dataDf['NA'] = dataDf['NA'].astype(str)
    # dataDf['SKIPPED'] = dataDf['SKIPPED'].astype(str)
    # dataDf['SKIPPED.1'] = dataDf['SKIPPED.1'].astype(str)
    # dataDf['SKIPPED.2'] = dataDf['SKIPPED.2'].astype(str)
    # dataDf['NA.1'] = dataDf['NA.1'].astype(str)
    # dataDf['RealTime'] = dataDf['RealTime'].astype(str)
    # dataDf['0'] = dataDf['0'].astype(str)
    # dataDf['SKIPPED.3'] = dataDf['SKIPPED.3'].astype(str)
    # dataDf['FALSE'] = dataDf['FALSE'].astype(str)
    # dataDf['NA.2'] = dataDf['NA.1'].astype(str)
    # dataDf['NA.3'] = dataDf['NA.1'].astype(str)
    # dataDf['NA.4'] = dataDf['NA.1'].astype(str)
    # dataDf['NA.5'] = dataDf['NA.1'].astype(str)
    # dataDf['isgiftbundle'] = dataDf['isgiftbundle'].astype(str)
    # dataDf['isrenewble' ] = dataDf['isrenewble'].astype(str)
    # dataDf['pcubad'] = dataDf['pcubad'].astype(str)
    # dataDf['actualcost'] = dataDf['actualcost'].astype(str)
    # dataDf['gifteenumber'] = dataDf['gifteenumber'].astype(str)
    # dataDf['expirydate'] = dataDf['expirydate'].astype(str)

    # dsnStr = cx_Oracle.makedsn("st-exa-scan.mtn.bj", "1521", service_name="MKT")
    # try :
    #     con = cx_Oracle.connect(user="PPREPORT", password="PsI4#sY5J#N1Y7#v", dsn=dsnStr)
    #     print("Connected to", con)
    #     cursor = con.cursor()
    #     dataInsertionTuples = [tuple(x) for x in dataDf.values]

    #     sqlTxt = "INSERT INTO MKTCONCEPT.CIS_BUNDLE_LOADING (FILEID, FILENAME, MSISDN, CHANNEL, CHANNELCODE, SHORTCODE, REQUESTID, \
    #     STARTTIME, ENDTIME, PRODUCTID, PRODUCTNAME, PRODUCTTYPE, \
    #     PRODUCTCONSTAINT, TRANSACTIONCODE, REFILLID, ORIGINOPERATORID, \
    #     CORRELATIONID, AMOUNTCHARGE, MOMOCHARGING, \"SUCCESS/FAILURE\", REASON, \
    #     NOTIFICATION, SPTYPE, NA,  SKIPPED, \"SKIPPED.1\", \"SKIPPED.2\", \"NA.1\", \
    #     REALTIME, \"0\", \"SKIPPED.3\", FALSE,\"NA.2\",\"NA.3\",\"NA.4\", \"NA.5\", ISGIFTBUNDLE, ISRENEWBLE, \
    #     PCUBAD, ACTUALCOST, GIFTEENUMBER, EXPIRYDATE) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, \
    #     :36, :37, :38, :39, :40, :41, :42)"

    #     # execute the sql to perform data extraction
    #     print("INSERTING DATA . . .")
    #     cursor.executemany(sqlTxt, [x for x in dataInsertionTuples])

    #     # commit the changes
    #     con.commit()

    #     print("Record inserted succesfully")
    # except cx_Oracle.DatabaseError as e:
    #     print("Problem connecting to Oracle", e)

    #     # Close the all database operation
    # finally:
    #     if cursor:
    #         cursor.close()
    #         print("Closing cursor")
    #     if con:
    #         con.close()
    #         print("Disconneted")


def automate():
    path_to_watch = '/Users/merveilleakogmail.com/Downloads/Automatic script/Automatic script'
    print('Your folder path is"',path_to_watch,'"')

    old = os.listdir(path_to_watch)
    print(old)

    while True:
        new = os.listdir(path_to_watch)
        if len(new) > len(old):
            newfile = list(set(new) - set(old))
            old = new
            extension = os.path.splitext(path_to_watch + "/" + newfile[0])[1]
            if extension == ".csv":
                print("New File:" + newfile[0]) 
                connectToBd(str(newfile[0]))
                print ("File ready for loading")
            else:
                continue            
        else:
            continue


def main():
    #connectToBd(str(""))

    automate()

main()