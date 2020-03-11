from timeit import default_timer as timer
from src.test.alaiza_project.API_service import APIService
from src.test.alaiza_project.database_service import DBService
import yaml
import src.test.alaiza_project.manager as manager
import time
from datetime import date


def main_zurich(arguments, logger):
    try:

        ##########CONFIG
        config = load_config('configuration/config_properties.yaml')
        mysql_host = config.get('DATABASE_host')
        mysql_port = config.get('DATABASE_port')
        mysql_db = config.get('DATABASE_db')
        mysql_user = config.get('DATABASE_user')
        mysql_passw = config.get('DATABASE_pass')
        mysql_tablename = config.get('DATABASE_table')
        email_api_key = config.get('EMAIL_api_key')
        email_secret_key = config.get('EMAIL_secret_key')
        email_version = config.get('EMAIL_version')

        daystored = date.today().strftime("%Y%m%d")

        freetoken = config.get('API_token')
        reptime = arguments.get('reptime')
        endingtime = arguments.get('exectime')
        timeend = int(time.time()) + endingtime*60  #timestamp minutes
        apiservice = APIService(freetoken)
        dbservice = DBService(mysql_host,mysql_port,mysql_user,mysql_passw,mysql_db,mysql_tablename)
        dbservice.createTable()
        dbservice.createMetaTable()

        RefreshMetadaTable(dbservice,apiservice)



        limitcoins = dbservice.getAvailableCurrencies()
        manager.storelogsBucket()

        while(time.time()<timeend):
            if( date.today().strftime("%Y%m%d") == daystored):
                #doextract
                start = timer()
                logger.info('Data will be generated')
                apiservice.update_data()
                dictdata = apiservice.get_all_data()
                dataframecleandata = manager.getCleanDataframe(limitcoins,dictdata,daystored)
                logger.info('Data will be stored')
                dbservice.storeDataFrame(dataframecleandata)
                logger.info('Data stored: '+str(len(dataframecleandata)))
                end = timer()
                diff = end - start
                if(diff < reptime*60):
                    logger.info('Sleeping: '+str(reptime*60-diff)+' seconds')
                    time.sleep(reptime*60 - diff)
            else:
                dataframestored = dbservice.GetDataframeDay(daystored)
                RefreshMetadaTable(dbservice, apiservice)
                manager.storeinBucket(daystored, dataframestored)
                manager.storelogsBucket()
                logger.info('Data stored into bucket')
                dbservice.storeExecution(daystored,len(limitcoins),len(dataframestored))
                daystored = date.today().strftime("%Y%m%d")
                limitcoins = dbservice.getAvailableCurrencies()
        end = timer()
        logger.info("Total time elapsed: %s seconds", (end - start))

    except Exception, ex:
        logger.critical("Error executing program", exc_info=True)
        exit(1)

    finally:
        logger.info("The program finished.")


def RefreshMetadaTable(dbservice,apiservice):
    dataframeprizeslastday = dbservice.GetLastDayPricesCounter()
    dictdata = apiservice.get_all_data()
    Lqueries = manager.GetQueriesForMetadata(dictdata, dataframeprizeslastday)
    dbservice.updateMetadata(Lqueries)



def load_config(config_file):
    with open(config_file, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

