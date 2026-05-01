# A demo of the local libraries

from trottertools.WriteOnceDict import WriteOnceDict
from trottertools.myDBTable import myDBTable
from trottertools.mySQLh import mySQLh


schemaDBT = myDBTable.myDBTable('information_schema','TABLES')

sql = f"SELECT * FROM {schemaDBT}" 

SQLh = mySQLh.mySQLh()

results = SQLh._conn.execute(sql)
for this_result in results:
    print(this_result)


sql_dict = WriteOnceDict.WriteOnceDict()


sql_dict['delete list first'] = f"DROP TABLE IF EXISTS mirrulation.this_table"

sql_dict['this_is a test'] = f"""
CREATE TABLE mirrulation.this_table AS 
SELECT * FROM {schemaDBT}
"""


is_just_print = False
SQLh.runQuerys(sql_dict)

