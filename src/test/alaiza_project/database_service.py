import logging
import mysql.connector

from sqlalchemy import create_engine


_logger = logging.getLogger(__name__)

class DBService:

    def __init__(self, host,port,user,passw,schema):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__pass = passw
        self.__sche = schema
        try:
            #self.__conn = create_engine('mysql+pymysql://' + user + ':' + passw + '@' + host + ':' + str(port) + '/' + schema, echo=False)
            self.__cnx = mysql.connector.connect(user=user, password=passw,
                                          host=host,
                                          database=schema)
            self.__cursor = self.__cnx.cursor()
        except:
            raise Exception('Unable to connect to MySQL connector')



    def executeFixedCost(self):
        cursor = self.__cnx.cursor()
        sql = """select reporting_period_start,reporting_period_end,reporting_period_length,sum(fixed_cost) 
            from  {0}.reporting_periods report
            left join
            (
            select 
                plan,
                supply_date_start,
                supply_date_end,amount, 
                DATEDIFF(supply_date_end,supply_date_start) as dates_diff ,
                amount/DATEDIFF(supply_date_end,supply_date_start) as fixed_cost,
                selected_date 
            from {0}.invoices a
            left join {0}.dim_dates b
            on b.selected_date between a.supply_date_start and a.supply_date_end
            ) fixed_cost
            on fixed_cost.selected_date between report.reporting_period_start and report.reporting_period_end
            group by reporting_period_start,reporting_period_end,reporting_period_length
        """.format(self.__sche)
        print (sql)
        cursor.execute(sql)
        _logger.info('query executed')
        return self.__cursor.fetchall()


    def executeStartBasedCost(self):
        cursor = self.__cnx.cursor()
        sql = """
            select 
	        a.reporting_period_start,
	        a.reporting_period_end, 
	        SUM(amount) as amount
	        from {0}.reporting_periods a 
	        left join {0}.invoices b on b.supply_date_start between a.reporting_period_start and a.reporting_period_end
	        group by a.reporting_period_start,a.reporting_period_end
	        """.format(self.__sche)
        print (sql)
        cursor.execute(sql)
        _logger.info('query executed')
        return self.__cursor.fetchall()

