from timeit import default_timer as timer
from src.test.alaiza_project.database_service import DBService
import yaml
import src.test.alaiza_project.manager as manager
import time
import time
from datetime import datetime

import sys


def main_zurich(arguments, logger):
    try:

        ##########CONFIG
        config = load_config('configuration/config_properties.yaml')
        mysql_host = config.get('DATABASE_host')
        mysql_port = config.get('DATABASE_port')
        mysql_db = config.get('DATABASE_db')
        mysql_user = config.get('DATABASE_user')
        mysql_passw = config.get('DATABASE_pass')

        ##########PARAMETERIZED
        costtype = arguments.get('costtype')
        tocsv = arguments.get('tocsv')


        ##########CONNECTIONS
        dbservice = DBService(mysql_host,mysql_port,mysql_user,mysql_passw,mysql_db)

        start_time = time.time()
        now = datetime.now()
        dtnow_string = now.strftime("%Y%m%d_%H%M%S")
        if costtype == 'fixed':
            logger.info('executing fixed costs process...')
            df = dbservice.executeFixedCost()
            logger.info('fixed costs process DONE')
        elif costtype == 'startbased':
            logger.info('executing costs based on start date process...')
            df = dbservice.executeStartBasedCost()
            logger.info('costs based on start date DONE')
        else:
            logger.critical('input method not rcognized...exiting')
            sys.exit(1)

        if tocsv == 'yes':
            manager.exportToCSV(df,costtype,dtnow_string)

        else:
            logger.info('Data is not going to be exported')
    except:
        logger.critical("something went really bad")
    finally:
        print("--- %s seconds ---" % (time.time() - start_time))



def load_config(config_file):
    with open(config_file, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

