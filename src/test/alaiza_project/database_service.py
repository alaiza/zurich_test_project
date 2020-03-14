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
            cnx = mysql.connector.connect(user=user, password=passw,
                                          host=host,
                                          database=schema)
            self.__cursor = cnx.cursor()
        except:
            raise Exception('Unable to connect to MySQL connector')



    def executeFixedCost(self):
        x = self.__cursor.execute("""
            select reporting_period_start,reporting_period_end,reporting_period_length,sum(fixed_cost) 
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
        )
        _logger.info('create table executed')


    def executeStartBasedCost(self):
        self.__cursor.execute("""
            select 
                a.reporting_period_start,
                a.reporting_period_end, 
                SUM(amount) 
            from smallpdf.reporting_periods a 
            left join smallpdf.invoices b 
            on b.supply_date_start between a.reporting_period_start and a.reporting_period_end
            group by a.reporting_period_start,a.reporting_period_end
                )""".format(self.__sche)
                )
        _logger.info('create table executed')


    def getAvailableCurrencies(self):
        L=[]
        sql = """select ID from crypto.metadata_currencies where activate = 'Y' or alive = 'Y'"""
        respons = self.__conn.execute(sql)
        dataframe = respons.fetchall()
        for a in dataframe:
            L.append(a[0])
        return L


    def createMetaTable(self):
        self.__conn.execute("""
            CREATE TABLE if not exists `metadata_currencies` (
                `ID` varchar(20) DEFAULT NULL,
                `Name` varchar(20) DEFAULT NULL,
                `Activate` varchar(20) DEFAULT NULL,
                `alive` varchar(20) DEFAULT NULL,
                `last_eur_prize` float 
                )"""
                )
        _logger.info('create table executed')

    def GetDataframeDay(self,daystored):
        query = """select * from {0}.{1} where extract_date = {2}""".format(self.__sche,self.__table,daystored)
        response = self.__conn.execute(query)
        dataframe = response.fetchall()
        return dataframe

    def GetLastDayPricesCounter(self):
        query = """select ID,last_eur_prize,counter_dead,alive from {0}.metadata_currencies""".format(self.__sche)
        response = self.__conn.execute(query)
        dataframe = response.fetchall()
        return dataframe




    def storeDataFrame(self,dataframecleandata):
        print('a')
        Lqueries = []
        for index, row in dataframecleandata.iterrows():
            sql = """insert into cryptowarehouse.currency_{0} values ({1},{2},{3})""".format(row['ID'],row['Timestamp_tm'],row['Price_eur'],row['Volume_24h'])
            Lqueries.append(sql)
        print('a')
        self.updateMetadata(Lqueries)
        #dataframecleandata.to_sql(name=self.__table, con=self.__conn, if_exists = 'append', index=False)
        _logger.info('data stored')

    def updateMetadata(self,Lqueries):
        counter = 0
        for a in Lqueries:
            counter = counter+1
            print(counter)
            self.__conn.execute(a)

    def storeExecution(self,daystored,len_limitcoins,len_dataframestored):
        sql = """insert into crypto.ExecutionMetaTable values (
        {0},
        {1},
        {2}
        )""".format(daystored,len_limitcoins,len_dataframestored)
        print sql
        self.__conn.execute(sql)





