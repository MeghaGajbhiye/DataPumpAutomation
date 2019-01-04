import os
import subprocess
import sys

'''Setting database environment'''
def set_db_env(dbenv):
	# set up some environment variables
	dbenv["PATH"] = os.environ["PATH"]
	print ("dbenv path is " + dbenv["PATH"])
	dbenv["ORACLE_HOME"] = os.environ["ORACLE_HOME"]
	print ("home is " + dbenv["ORACLE_HOME"])
	dbenv["TNS_ADMIN"] = os.environ["TNS_ADMIN"]
	print ("tns is " + dbenv["TNS_ADMIN"])
	dbenv["LD_LIBRARY_PATH"] = os.environ["LD_LIBRARY_PATH"]
	print ("lib is " + dbenv["LD_LIBRARY_PATH"])

'''DB export'''
def db_export():
	db_user = sys.argv[1]
	db_pass = sys.argv[2]
	db_alias = sys.argv[3]
	table = sys.argv[4]
	dump_file = sys.argv[5]
	expdp_args = db_user + '/' + db_pass + '@' + db_alias + ' tables=' + table + ' DUMPFILE=' + dump_file + ' DIRECTORY=DATA_PUMP_DIR'
	db_exp_work = subprocess.Popen(["expdp", expdp_args], 
				stdout=subprocess.PIPE, env={"PATH": os.environ["PATH"], "ORACLE_HOME": os.environ["ORACLE_HOME"], "TNS_ADMIN": os.environ["TNS_ADMIN"], "LD_LIBRARY_PATH": os.environ["LD_LIBRARY_PATH"] })
	if db_exp_work.wait() != 0:
		print "db_export() failed!"
		raise Exception("db_export() failed!")
	print "db_export() end!"

'''Printing help function'''
def help():
	print "Usage : %s db_user db_pass db_alias table_to_export dump_file" 
	#%(sys.argv[0])

if __name__=='__main__':
	if len(sys.argv) != 6:
		help()
		sys.exit(1)
	#dbenv = {}
	#set_db_env(dbenvi)
	db_export()
