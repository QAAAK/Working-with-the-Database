
#connection oracle


import oracledb
import os


un = os.environ.get('PYTHON_USERNAME')                                              
pw = os.environ.get('PYTHON_PASSWORD')
cs = os.environ.get('PYTHON_CONNECTSTRING')
host = os.environ.get('HOST')
port = os.environ.get('PORT')
service_name = os.environ.get('SERVICE NAME')


connection = oracledb.connect(user=un, password=pw, dsn=cs, host=host, port=port, service_name=service_name)    #create connect datebase
pool = oracledb.create_pool(user=un, password=pw, dsn=cs, host=host, port=port, service_name=service_name,      #create pool; getmode = если порог max > 5, выдает ошибку)
                            min=1, max=4, increment=1, getmode=oracledb.POOL_GETMODE_NOWAIT)                    


cursor = connection.cursor()
