# -*- coding: utf-8 -*-
"""
DATA ENGINES PROJECT

@author: jorge.alaiza
-- Receive the path where files are located like: >>DATA_LOAD_xxxxxxx.py clientname dataprovider filetype       <- (client dataprovider filetype) 
**filetype is a sequence of subfolders separed by "-" for example: "AAAA-BBBB" means "AAAA/BBBB" 

python output:
    - if python ends fine: return 0
    - if python finds an exception : return 1
    - if python tryes to process empty path, returns 2
"""
import time
import sys
import datetime
import random
from dateutil import relativedelta
import csv

########################################SET UP
totalrows = sys.argv[1]
startdate = sys.argv[2]
enddate = sys.argv[3]

########################################METHODS
def generateEpoch(datestr):
    x = datetime.datetime(int(datestr.split('-')[0]),int(datestr.split('-')[1]),int(datestr.split('-')[2]),0,0)
    timestamp = time.mktime(x.timetuple()) + x.microsecond / 1e6
    return timestamp

def generateRandomDate(epochstart,epochend):
    return random.uniform(epochstart, epochend)

def generateRandomInsightTypology():
    if random.randint(0,1)==1:
        return 'yearly'
    else:
        return 'monthly'

def getDateInAYear(randomdate):
    datetostart = datetime.datetime.fromtimestamp(randomdate)
    return datetostart + relativedelta.relativedelta(months=12)

def getDateInAMonth(randomdate):
    datetostart = datetime.datetime.fromtimestamp(randomdate)
    return datetostart + relativedelta.relativedelta(months=1)

def exportToCSV(rows):
    fp = open("""./csv_export_invoices.csv""", 'wb')
    myFile = csv.writer(fp)
    myFile.writerows(rows)
    fp.close()
########################################START

def main():
    epochstart = generateEpoch(startdate)
    epochend = generateEpoch(enddate)
    rows = []
    rows.append(("'plan'","'supply_date_start'","'supply_date_end'","'amount'"))
    for a in range(int(totalrows)):
        randomdate = generateRandomDate(epochstart, epochend)
        startingdate = datetime.datetime.fromtimestamp(randomdate).strftime("'%Y-%m-%d %H:%M:%S'")
        typology = generateRandomInsightTypology()
        if typology == 'yearly':
            endingdate = getDateInAYear(randomdate).strftime("'%Y-%m-%d %H:%M:%S'")
            tuple = ("'yearly'", startingdate, endingdate,119)
        else:
            endingdate = getDateInAMonth(randomdate).strftime("'%Y-%m-%d %H:%M:%S'")
            tuple = ("'monthly'", startingdate, endingdate, 12)
        rows.append(tuple)
    exportToCSV(rows)
    print('process finished correctly')

if __name__ == "__main__":
    try:
        int(totalrows)
        datetime.datetime.strptime(startdate, '%Y-%m-%d')
        datetime.datetime.strptime(enddate, '%Y-%m-%d')
    except:
        print('Wrong parameters')
    try:
        main()
    except:
        print('Something whent wrong during execution')