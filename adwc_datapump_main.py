from __future__ import print_function



import cx_Oracle

import csv

import time

import datetime

import threading

pool = cx_Oracle.SessionPool("megha", "ETEAdwc12345678",

       "eteadwc_high", 2, 5, 1, threaded = True)

#csvf = open('ficob_data.csv', 'w')

#csv_writer = csv.writer(csvf,delimiter='|')

conn = pool.acquire()

cursor = conn.cursor()

#cursor.arraysize = 80000

print("TheLongQuery(): beginning execute...")



print("Starting to dump data to object storage!\n")

start_time = time.time()

now = datetime.datetime.now()

print("Start time :")

print (now)



cursor.execute("""

declare

  p_file utl_file.file_type;

begin



  p_file := utl_file.fopen( 'DATA_PUMP_DIR', 'test_file_megha.txt', 'w' );

  for c in (select * from sales WHERE ROWNUM <= 10)

  loop

    utl_file.put_line(p_file, c.cust_id );
  end loop;

  utl_file.fclose(p_file);

  dbms_cloud.put_object( 

    'OBJ_STORE_CRED', 

    'https://swiftobjectstorage.us-ashburn-1.oraclecloud.com/v1/gse00014638/ETEBucket/test_file_megha.txt',

    'DATA_PUMP_DIR',

    'test_file_megha.txt' );

    end;
                """)

print("TheLongQuery(): done execute...")



elapsed_time = time.time() - start_time

print("All done!\n")

now = datetime.datetime.now()

print("End time : ")

print (now)

print("Total time in seconds to finish : ")

print(elapsed_time)

print("\n")

class dataPump(object):


def main(args):
    """Main function"""

    datapump = DataPump(args)

class main():
    """Main class"""

    def __init__(self):
        print ("Inside init of parse values")
        parser = argparse.ArgumentParser(
            description="Add users information to do CRUD operation",
            usage='''python adwc_datapump_main.py <create, delete, view, validate> <enter an option>
                <-u, --username> <enter username>
                <-p, --password> <Enter Password>
                <-sn, --sname> <Service Name>
                <-r, --recipe> <Enter Recipe Name>''')

        acts = ['create', 'delete', 'view', 'scale', 'validate']
        parser.add_argument('action',
                            choices=acts,
                            help="Required Field")
        parser.add_argument("-u", "--username",
                           help="Enter the username",
                           default=None, required = True)
        parser.add_argument("-p", "--password",
                            help="Enter the password",
                            default=None, required = True)
        parser.add_argument("-sn", "--sname",
                            help="Enter the service name",
                            default=None, required = True)
        parser.add_argument("-adwc", "--autodb",
                            help="For Autonomous Cloud Service",
                            action = 'store_true')
        parser.add_argument("-c", "--ccs",
                            help="For Compute Cloud Service",
                            action = 'store_true')
        parser.add_argument("--debug",
                            help="print debug messages to stderr",
                            action = 'store_true')
        parser.add_argument("-r", "--recipe",
                            help="Add Recipe",
                            type = str,
                           required = True)
        args = parser.parse_args()
        main(args)

if __name__ == "__main__":
    ParseValues()

