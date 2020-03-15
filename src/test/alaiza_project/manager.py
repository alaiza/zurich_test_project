import logging
import pandas as pd
import subprocess
import csv
import time


_logger = logging.getLogger(__name__)



def exportToCSV(rows,costtype,dtnow_string):
    fp = open("""./output/csv_export_{1}_{0}.csv""".format(dtnow_string,costtype), 'w')
    myFile = csv.writer(fp)
    myFile.writerows(rows)
    fp.close()



def run_cmd(args_list):
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    return s_return, s_output, s_err


