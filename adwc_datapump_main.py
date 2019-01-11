from __future__ import print_function

import subprocess
import argparse
import base64
import json
import os
import cx_Oracle
import time
import datetime
import threading


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
        """Function to export"""

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

    def datapump_import(self):
        """Function to import"""
        #impdp_args = db_user + '/' + db_pass + '@' + db_alias + ' credential=OBJ_STORE_CRED' + ' tables=' + table + ' dumpfile=/u03/dbfs/7CCB13275D285150E0531A10000A7E92/data/dpdump/test_export_python1.dmp' + ' DIRECTORY=DATA_PUMP_DIR' + ' transform=segment_attributes:n transform=dwcs_cvt_iots:y transform=constraint_use_default_index:y exclude=index, cluster, indextype, materialized_view, materialized_view_log, materialized_zonemap, db_link'
        adwc_impdp = self.username + '/' + self.password + '@' + self.service_name + ' tables=' + self.table + ' credential=' + self.cred + ' DUMPFILE=' + self.object_storage + ' DIRECTORY=DATA_PUMP_DIR' + ' transform=segment_attributes:n transform=dwcs_cvt_iots:y transform=constraint_use_default_index:y exclude=index, cluster, indextype, materialized_view, materialized_view_log, materialized_zonemap, db_link'
        adwc_import = subprocess.Popen(["impdp", adwc_impdp],
                                       stdout=subprocess.PIPE,
                                       env=self.adwc_env)
        if adwc_import.wait() != 0:
            print
            "adwc_import() failed!"
            raise Exception("adwc_import() failed!")
        print ("adwc_import() end!")
        time.sleep(10)

    def sqlplus(self):
        port = raw_input("Enter port number : ")
        host = raw_input("Enter host number : ")
        service_name = raw_input("Enter service name : ")
        username = raw_input("Enter username : ")
        password = raw_input("Enter password : ")
        service = str(('(description= (address=(protocol=tcps)(port=%s)(host=%s))(connect_data=(service_name=%s))(security=(ssl_server_cert_dn="CN=adwc.uscom-east-1.oraclecloud.com,OU=Oracle BMCS US,O=Oracle Corporation,L=Redwood City,ST=California,C=US")))'
                       %(port, host, service_name)))
        adwc_impdp = username + '/' + password + '@' + "'" + service + "'"
        print ("*****************************************")
        print (adwc_impdp)
        adwc_import = subprocess.Popen(["sqlplus", adwc_impdp],
                                       stdout=subprocess.PIPE,
                                       env=self.adwc_env)
        if adwc_import.wait() != 0:
            print
            "sqlplus() failed!"
            raise Exception("sqlplus() failed!")
        print("sqlplus() end!")
        time.sleep(10)


    def datapump_import1(self):
        """Function to import"""
        port = raw_input("Enter port number : ")
        host = raw_input("Enter host number : ")
        service_name = raw_input("Enter service name : ")
        username = raw_input("Enter username : ")
        password = raw_input("Enter password : ")
        service = str(('(description= (address=(protocol=tcps)(port=%s)(host=%s))(connect_data=(service_name=%s))(security=(ssl_server_cert_dn="CN=adwc.uscom-east-1.oraclecloud.com,OU=Oracle BMCS US,O=Oracle Corporation,L=Redwood City,ST=California,C=US")))'
                       % (port, host, service_name)))
        adwc_impdp = username + '/' + password + '@' + "'" + service + "'" + ' tables=' + self.table + ' credential=' + self.cred + ' DUMPFILE=' + self.object_storage + ' DIRECTORY=DATA_PUMP_DIR' + ' transform=segment_attributes:n transform=dwcs_cvt_iots:y transform=constraint_use_default_index:y exclude=index, cluster, indextype, materialized_view, materialized_view_log, materialized_zonemap, db_link'
        adwc_import = subprocess.Popen(["impdp", adwc_impdp],
                                       stdout=subprocess.PIPE,
                                       env=self.adwc_env)
        if adwc_import.wait() != 0:
            print
            "adwc_import() failed!"
            raise Exception("adwc_import() failed!")
        print ("adwc_import() end!")
        time.sleep(10)

    def moveTableToTextOS(self):
        pool = cx_Oracle.SessionPool(self.username, self.password, self.service_name, 2, 5, 1, threaded=True)
        conn = pool.acquire()
        cursor = conn.cursor()
        print("TheLongQuery(): beginning execute...")
        print("Starting to dump data to object storage!\n")
        start_time = time.time()
        now = datetime.datetime.now()

        print("Start time : ")

        print(now)
        print(self.dump_file, self.cred, self.object_storage, self.dump_file)
        textfile = str(self.table + ".txt")
        bind_vars = {"dump_file": str(self.dump_file), "cred": str(self.cred),
                     "object_storage": str(self.object_storage), "textfile": textfile, "table": self.table}


        sql = """ declare
                p_file utl_file.file_type;
                begin
                p_file := utl_file.fopen( 'DATA_PUMP_DIR', :textfile  , 'w' );
                for c in (select * from :table WHERE ROWNUM <= 10)
                loop
                    utl_file.put_line(p_file, c.cust_id );
                    end loop;
                utl_file.fclose(p_file);
                dbms_cloud.put_object( 
                    :cred, 
                    :object_storage,
                    'DATA_PUMP_DIR',
                    :dump_file );
                    end;
                """

        cursor.execute(sql, bind_vars)
        print("TheLongQuery(): done execute...")
        elapsed_time = time.time() - start_time
        print("All done!\n")
        now = datetime.datetime.now()
        print("End time : ")
        print(now)
        print("Total time in seconds to finish : ")
        print(elapsed_time)
        print("\n")


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
        print (self.dump_file, self.cred, self.object_storage, self.dump_file)
        bind_vars = {"dump_file":str(self.dump_file), "cred":str(self.cred), "object_storage":str(self.object_storage) }
        sql="""BEGIN
        DBMS_CLOUD.PUT_OBJECT(
        credential_name => :cred,
        object_uri => :object_storage,
        directory_name  => 'DATA_PUMP_DIR',
        file_name => :dump_file);
        END;
        """
        cursor.execute(sql, bind_vars)

        print("TheLongQuery(): done execute...")
        elapsed_time = time.time() - start_time
        print("All done!\n")
        now = datetime.datetime.now()
        print("End time : ")
        print(now)
        print("Total time in seconds to finish : ")
        print(elapsed_time)
        print("\n")
        print ("time.sleep(5)")
        time.sleep(10)

def main(args):
    """Main function"""

    datapump = DataPump(args)
    #datapump.datapump_export()
    #datapump.movefile_os()
    # datapump.datapump_import()
    datapump.sqlplus()

class ParseValues(object):
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

        # acts = ['export', 'import', 'move']
        # parser.add_argument('action',
        #                     choices=acts,
        #                     help="Required Field")

        parser.add_argument("-u", "--username",
                           help="Enter the username",
                           default=None, required = True)
        parser.add_argument("-p", "--password",
                            help="Enter password",
                            default=None, required=True)
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
        #parser.add_argument("-adwc", "--adwc",
        #                    help="For Autonomous Cloud Service",
        #                    action = 'store_true')
        #parser.add_argument("--debug",
        #                    help="print debug messages to stderr",
        #                    action = 'store_true')
        args = parser.parse_args()
        main(args)

if __name__ == "__main__":
    ParseValues()

