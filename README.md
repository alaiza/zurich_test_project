# zurich_test_project
This is a test to check my performance



1 Code
===============

<h3> SCRIPT </h3>

<p> You can find the script Zurich_synthetic_invoices.py, you can execute it like Zurich_synthetic_invoices.py 500 2020-01-01 2020-12-31, being the first parameter the number of lines and the 2nd and 3rd the dates in between to generate random start_dates, the csv file will appear in the same path </p>


<h3> PROJECT </h3>

<p>You can execute the code executing the launcher.py with the parameters</p>

1. costtype: choices=[fixed,startbased], how yo want to calculate the benefits, dividing all the amount per day of availability or assuming the entire payment on the start date

2. tocsv: choices=[yes,no], if you want to generate in "output" folder a csv file with the results

Notice: you can find examples into output folder**
<br>
<br>
<br>
<br>
2 Query
===============


>####Setting up MySQL

>>create user 'zurich_test'@'localhost' identified by 'zurich_test';

>>grant all privileges on *.* to 'zurich_test'@'localhost';

>>alter user 'zurich_test'@'localhost' identified with mysql_native_password by 'zurich_test';

>>create database smallpdf;

>>use smallpdf;

Now I will insert the records, the volume dont require external tables or processes to insert by batches

>####Reporting periods creation
>
>create table smallpdf.reporting_periods (
>reporting_period_start date,
>reporting_period_end date,
>reporting_period_length int
>)

Adding rows...

>####Insertion
>
>insert into smallpdf.reporting_periods values ('2019-01-01 00:00:00','2019-02-01 00:00:00',31);
>insert into smallpdf.reporting_periods values ('2019-02-01 00:00:00','2019-03-01 00:00:00',28);
>insert into smallpdf.reporting_periods values ('2019-03-01 00:00:00','2019-04-01 00:00:00',31);
>insert into smallpdf.reporting_periods values ('2019-04-01 00:00:00','2019-05-01 00:00:00',30);
>insert into smallpdf.reporting_periods values ('2019-05-01 00:00:00','2019-06-01 00:00:00',31);
>insert into smallpdf.reporting_periods values ('2019-06-01 00:00:00','2019-07-01 00:00:00',30);
>insert into smallpdf.reporting_periods values ('2019-07-01 00:00:00','2019-08-01 00:00:00',31);
>insert into smallpdf.reporting_periods values ('2019-08-01 00:00:00','2019-09-01 00:00:00',31);
>insert into smallpdf.reporting_periods values ('2019-09-01 00:00:00','2019-10-01 00:00:00',30);
>insert into smallpdf.reporting_periods values ('2019-10-01 00:00:00','2019-11-01 00:00:00',31);
>insert into smallpdf.reporting_periods values ('2019-11-01 00:00:00','2019-12-01 00:00:00',30);
>insert into smallpdf.reporting_periods values ('2019-12-01 00:00:00','2020-01-01 00:00:00',31);


>####Invoices table creation
>
>create table smallpdf.invoices (
>plan varchar(20),
>supply_date_start date,
>supply_date_end date,
>amount integer 
>)

Adding rows (example)

>####example
>
>insert into smallpdf.invoices values ('yearly','2019-12-31 00:00:00','2020-12-31 00:00:00',119);
>insert into smallpdf.invoices values ('yearly','2019-12-31 00:00:00','2020-12-31 00:00:00',119);
>insert into smallpdf.invoices values ('monthly','2019-12-31 00:00:00','2020-01-31 00:00:00',12);
>insert into smallpdf.invoices values ('monthly','2019-12-31 00:00:00','2020-01-31 00:00:00',12);
>insert into smallpdf.invoices values ('monthly','2019-12-31 00:00:00','2020-01-31 00:00:00',12);
>insert into smallpdf.invoices values ('monthly','2019-12-31 00:00:00','2020-01-31 00:00:00',12);
>insert into smallpdf.invoices values ('yearly','2019-12-31 00:00:00','2020-12-31 00:00:00',119);
>insert into smallpdf.invoices values ('monthly','2019-12-31 00:00:00','2020-01-31 00:00:00',12);
>insert into smallpdf.invoices values ('yearly','2019-12-31 00:00:00','2020-12-31 00:00:00',119);
>insert into smallpdf.invoices values ('yearly','2019-12-31 00:00:00','2020-12-31 00:00:00',119);
>insert into smallpdf.invoices values ('yearly','2019-12-31 00:00:00','2020-12-31 00:00:00',119);
>insert into smallpdf.invoices values ('yearly','2019-12-31 00:00:00','2020-12-31 00:00:00',119);
>...(more)

Create auxiliar table

>####creating dimension table for dates
>
>This table was created to generate all the days between two given dates and divide the benefit between months
>
>create table dim_dates as (select adddate('1970-01-01',t4.i*10000 + t3.i*1000 + t2.i*100 + t1.i*10 + t0.i) selected_date from
>(select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t0,
>(select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t1,
>(select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t2,
>(select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t3,
>(select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t4) v
>where selected_date between '2018-01-01' and '2025-01-01'

First query generate, you can execute with the parameter costtype = startbased

>####if we consider full payment on start_date
>
>select 
>a.reporting_period_start,
>a.reporting_period_end, 
>SUM(amount) as amount
>from smallpdf.reporting_periods a 
>left join smallpdf.invoices b on b.supply_date_start between a.reporting_period_start and a.reporting_period_end
>group by a.reporting_period_start,a.reporting_period_end

Second query generate, you can execute with the parameter costtype = fixed

>####if we consider the benefit divided between all the subscription dates
>
>select reporting_period_start,reporting_period_end,reporting_period_length,sum(fixed_cost) from smallpdf.reporting_periods report
>left join
>(
>select plan,supply_date_start,supply_date_end,amount, DATEDIFF(supply_date_end,supply_date_start) as dates_diff ,amount/DATEDIFF(supply_date_end,supply_date_start) as fixed_cost,selected_date from smallpdf.invoices a
>left join 
>smallpdf.dim_dates b
>on b.selected_date between a.supply_date_start and a.supply_date_end
>) fixed_cost
>on fixed_cost.selected_date between report.reporting_period_start and report.reporting_period_end
>group by reporting_period_start,reporting_period_end,reporting_period_length

