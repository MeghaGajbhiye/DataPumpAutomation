from __future__ import print_function
import os
import subprocess
import sys


import cx_Oracle

import csv

import time

import datetime

import threading
#
# pool = cx_Oracle.SessionPool("megha", "ETEAdwc12345678",
#
#        "eteadwc_high", 2, 5, 1, threaded = True)
#
# #csvf = open('ficob_data.csv', 'w')
#
# #csv_writer = csv.writer(csvf,delimiter='|')
#
# conn = pool.acquire()
#
# cursor = conn.cursor()
#
# #cursor.arraysize = 80000
#
# print("TheLongQuery(): beginning execute...")
#
#
#
# print("Starting to dump data to object storage!\n")
#
# start_time = time.time()
#
# now = datetime.datetime.now()
#
# print("Start time :")
#
# print (now)
#
#
#
# cursor.execute("""
#
# declare
#
#   p_file utl_file.file_type;
#
# begin
#
#
#
#   p_file := utl_file.fopen( 'DATA_PUMP_DIR', 'test_file_megha.txt', 'w' );
#
#   for c in (select * from sales WHERE ROWNUM <= 10)
#
#   loop
#
#     utl_file.put_line(p_file, c.cust_id );
#   end loop;
#
#   utl_file.fclose(p_file);
#
#   dbms_cloud.put_object(
#
#     'OBJ_STORE_CRED',
#
#     'https://swiftobjectstorage.us-ashburn-1.oraclecloud.com/v1/gse00014638/ETEBucket/test_file_megha.txt',
#
#     'DATA_PUMP_DIR',
#
#     'test_file_megha.txt' );
#
#     end;
#                 """)
#
# print("TheLongQuery(): done execute...")
#
#
#
# elapsed_time = time.time() - start_time
#
# print("All done!\n")
#
# now = datetime.datetime.now()
#
# print("End time : ")
#
# print (now)
#
# print("Total time in seconds to finish : ")
#
# print(elapsed_time)
#
# print("\n")

class DataPump(object):
    """Class for DataPump"""
    def __init__(self, args):
        self.username = args.username
        self.password = args.password
        self.service_name = args.sname
        self.table = args.table
        self.dump_file = args.dumpfile
        self.cred = args.cred
        self.object_storage= args.os
        self.args = args
        self.adwc_env={}
        self.adwc_env["PATH"] = os.environ["PATH"]
        print("adwc_env path is " + self.adwc_env["PATH"])
        self.adwc_env["ORACLE_HOME"] = os.environ["ORACLE_HOME"]
        print("home is " + self.adwc_env["ORACLE_HOME"])
        self.adwc_env["TNS_ADMIN"] = os.environ["TNS_ADMIN"]
        print("tns is " + self.adwc_env["TNS_ADMIN"])
        self.adwc_env["LD_LIBRARY_PATH"] = os.environ["LD_LIBRARY_PATH"]
        print("lib is " + self.adwc_env["LD_LIBRARY_PATH"])

    def datapump_export(self):
        adwc_expdp = self.username + '/' + self.password + '@' + self.service_name + ' tables=' + self.table + ' DUMPFILE=' + self.dump_file + ' DIRECTORY=DATA_PUMP_DIR'
        adwc_export = subprocess.Popen(["expdp", adwc_expdp],
                                       stdout=subprocess.PIPE,
                                       env=self.adwc_env)
        if adwc_export.wait() != 0:
            print
            "adwc_export() failed!"
            raise Exception("adwc_export() failed!")
        print
        "adwc_export() end!"

    def movefile_os(self):
        pool = cx_Oracle.SessionPool(self.username, self.password,self.service_name, 2, 5, 1, threaded=True)
        conn = pool.acquire()
        cursor = conn.cursor()
        print("TheLongQuery(): beginning execute...")
        print("Starting to dump data to object storage!\n")
        start_time = time.time()
        now = datetime.datetime.now()

        print("Start time : ")

        print(now)

        cursor.execute("""
        declare
        p_file utl_file.file_type;
        begin
        p_file := utl_file.fopen( 'DATA_PUMP_DIR', %s , 'w' );
        for c in (select * from sales WHERE ROWNUM <= 10)
        loop
            utl_file.put_line(p_file, c.cust_id );
            end loop;
        utl_file.fclose(p_file);
        dbms_cloud.put_object( 
            %s, 
            %s,
            'DATA_PUMP_DIR',
            %s );
            end;
        """, (self.dump_file, self.cred, self.Object_storage, self.dump_file))

        print("TheLongQuery(): done execute...")
        elapsed_time = time.time() - start_time
        print("All done!\n")
        now = datetime.datetime.now()
        print("End time : ")
        print(now)
        print("Total time in seconds to finish : ")
        print(elapsed_time)
        print("\n")

def main(args):
    """Main function"""

    datapump = DataPump(args)
    datapump.datapump_export()
    datapump.movefile_os()

class ParseValues():
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
        parser.add_argument("-t", "--table",
                            help="Enter the tablename",
                            default=None, required=True)
        parser.add_argument("-f", "--dumpfile",
                            help="Enter the filename",
                            default=None, required=True)
        parser.add_argument("-c", "--cred",
                            help="Enter the credential name",
                            default=None, required=True)
        parser.add_argument("-o", "--os",
                            help="Enter the object storage swift url",
                            default=None, required = True)
        parser.add_argument("-sn", "--sname",
                            help="Enter the service name",
                            default=None, required = True)
        parser.add_argument("-adwc", "--adwc",
                            help="For Autonomous Cloud Service",
                            action = 'store_true')
        parser.add_argument("-c", "--ccs",
                            help="For Compute Cloud Service",
                            action = 'store_true')
        parser.add_argument("--debug",
                            help="print debug messages to stderr",
                            action = 'store_true')
        # parser.add_argument("-r", "--recipe",
        #                     help="Add Recipe",
        #                     type = str,
        #                    required = True)
        args = parser.parse_args()
        main(args)

if __name__ == "__main__":
    ParseValues()

