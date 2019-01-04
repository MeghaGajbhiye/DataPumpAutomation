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


