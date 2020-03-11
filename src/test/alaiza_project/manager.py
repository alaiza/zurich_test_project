import logging
import pandas as pd
import subprocess
import csv
import time


_logger = logging.getLogger(__name__)

def getCleanDataframe(listcoins,dictdata,daystored):
    data=[]
    for key in dictdata:
        if(key in listcoins):
            auxrow = []
            auxrow.append(daystored)
            auxrow.append(key)
            auxrow.append(str(dictdata[key].get('Price_cny')))
            auxrow.append(str(dictdata[key].get('Name')))
            auxrow.append(str(dictdata[key].get('Timestamp')))
            auxrow.append(str(dictdata[key].get('Price_gbp')))
            auxrow.append(str(dictdata[key].get('Label')))
            auxrow.append(str(dictdata[key].get('Price_rur')))
            auxrow.append(str(float(dictdata[key].get('Price_btc'))))
            auxrow.append(str(dictdata[key].get('Price_usd')))
            auxrow.append(str(dictdata[key].get('Volume_24h')))
            auxrow.append(str(dictdata[key].get('Price_eur')))
            data.append(auxrow)
    df = pd.DataFrame(data, columns=['extract_date','ID', 'Price_cny','Name_nm','Timestamp_tm','Price_gbp','Label','Price_rur','Price_btc','Price_usd','Volume_24h','Price_eur'])
    return df

def storeinBucket(daystored,dataframestored):
    (ret, current_path, err) = run_cmd(['pwd'])
    current_path = current_path.replace('\n','/')
    namefile = 'crypto_data_'+daystored+'.csv'
    with open(current_path+'output/'+namefile, 'wb') as writeFile:
        writer = csv.writer(writeFile)
        writer.writerows(dataframestored)
    _logger.info('Exporting into bucket')
    (ret, out, err) = run_cmd(['gsutil','cp',current_path+'output/'+namefile,'gs://crypto-alaiza-project/data/crypto-WorldCoinIndex/'])
    if(ret==1):
        _logger.error('Exporting into bucket FAILED, maintaining the file: '+current_path + 'output/' + namefile)
    else:
        _logger.info('cleaning file: '+current_path + 'output/' + namefile)
        (ret, out, err) = run_cmd(['rm', current_path + 'output/' + namefile])

def storelogsBucket():
    (ret, current_path, err) = run_cmd(['pwd'])
    current_path = current_path.replace('\n', '/')
    (ret, files, err) = run_cmd(['ls', current_path + '/logs/'])
    Lfiles = files.split('\n')
    Lauxfiles = []
    for a in Lfiles:
        if ('crypto_logfile.log.' in a):
            Lauxfiles.append(a)
    for a in Lauxfiles:
        (ret, out, err) = run_cmd(['gsutil', 'cp', current_path + '/logs/'+a,'gs://crypto-alaiza-project/logs/crypto-WorldCoinIndex/'])
        (ret, out, err) = run_cmd(['rm', current_path + '/logs/' + a])
    print('done')


def GetQueriesForMetadata(fulldict, lastpricescounterdataframe):
    fulldictkeys = fulldict.keys()
    dictaux = {}
    for a in lastpricescounterdataframe:
        dictaux[a[0]] = a[1:]

    newcurrencies = []
    newupdates = []
    addcounter = []
    killcurrencies = []
    for a in fulldictkeys:
        if (str(a) not in dictaux.keys()):
            newcurrencies.append([a,fulldict.get(a).get('Name'),fulldict.get(a).get('Price_eur'),fulldict.get(a).get('Volume_24h')])
        else:
            price = fulldict.get(a).get('Price_eur')
            priceold = dictaux.get(a)[0]
            if (price!=priceold):
                newupdates.append([a,fulldict.get(a).get('Name'),price,fulldict.get(a).get('Volume_24h'),round(((price * 100)/priceold)-100, 8)])
            else:
                if(dictaux.get(a)[1] >=3 and dictaux.get(a)[2] == 'Y'):
                    killcurrencies.append(a)
                elif(dictaux.get(a)[1] < 3 and dictaux.get(a)[2] == 'Y'):
                    addcounter.append([a,int(dictaux.get(a)[1])+1])
    Lqueries = GetQueriesFromLists(newcurrencies,newupdates,addcounter,killcurrencies)
    return Lqueries


def GetQueriesFromLists(newcurrencies,newupdates,addcounter,killcurrencies):
    L = []
    epoch_time = str(int(time.time()))
    for a in newcurrencies:
        L.append("""
        Create table if not exists cryptowarehouse.currency_{0} (
        id INT NOT NULL AUTO_INCREMENT,
        time_stamp integer,
        price_eur float,
        Volume_24h float,
        PRIMARY KEY (id)
        )AUTO_INCREMENT=1""".format(a[0]))
        L.append("""insert into crypto.metadata_currencies values ('{0}','{1}','Y','Y',{2},{3},0,0,{4},0)""".format(a[0],a[1],a[2],a[3],epoch_time))
    for a in newupdates:
        L.append("""update crypto.metadata_currencies set last_eur_prize = {1} ,alive = 'Y', Volume_24 = {2}, counter_dead = 0,percent_diff_value = {3}  where ID = '{0}'""".format(a[0],a[2],a[3],a[4]))
    for a in killcurrencies:
        L.append("""update crypto.metadata_currencies set alive = 'N',counter_dead = 0,epoch_dead = {1} ,percent_diff_value=0 where ID = '{0}'""".format(a,epoch_time))
        L.append("""drop table if exists cryptowarehouse.currency_{0}""".format(a))
    for a in addcounter:
        L.append("""update crypto.metadata_currencies set counter_dead = {1}  where ID = '{0}'""".format(a[0],a[1]))
    return L

def run_cmd(args_list):
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    return s_return, s_output, s_err


