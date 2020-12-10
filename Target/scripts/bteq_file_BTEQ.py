import os

import sys

currentDirectory = os.path.dirname(os.path.realpath(__file__))
rootDirectory = os.path.abspath(os.path.join(currentDirectory, "."))
## Adding parent dir to paths
 
sys.path.append(rootDirectory)
import snowconvert_helpers

from snowconvert_helpers import Export

# Populating the table.
def LABEL_1():
   snowconvert_helpers.execute_sql_statement("""INSERT INTO DEMO_DB.DEMO_DB.TABLE1 VALUES (1,1,'22')""", con)
   snowconvert_helpers.execute_sql_statement("""INSERT INTO DEMO_DB.DEMO_DB.TABLE1 VALUES (2,2,'33')""", con)
   snowconvert_helpers.execute_sql_statement("""INSERT INTO DEMO_DB.DEMO_DB.TABLE1 VALUES (3,3,'44')""", con)

   if snowconvert_helpers.error_level > 0:
      ABEND()
   LABEL_2()

def LABEL_2():
   snowconvert_helpers.execute_sql_statement("""SELECT
*
FROM DEMO_DB.DEMO_DB.TABLE1""", con)
   GOODEND()

# Finish.
def GOODEND():
   snowconvert_helpers.quit_application()
   ABEND()

def ABEND():
   snowconvert_helpers.quit_application(snowconvert_helpers.error_code)
con = None

try:
   snowconvert_helpers.configure_log()
   con = snowconvert_helpers.log_on()
   # Error handlers.
   snowconvert_helpers.set_error_level([3807], 0)

   if snowconvert_helpers.error_level > 0:
      ABEND()

   # Setup.
   snowconvert_helpers.execute_sql_statement("""drop table DEMO_DB.PUBLIC.TABLE1""", con)
   snowconvert_helpers.execute_sql_statement("""drop table DEMO_DB.PUBLIC.TABLE2""", con)
   snowconvert_helpers.execute_sql_statement("""drop table DEMO_DB.PUBLIC.TABLE3""", con)
   snowconvert_helpers.set_error_level([3807], 8)

   if snowconvert_helpers.error_level > 0:
      ABEND()
   snowconvert_helpers.set_error_level([3807], 0)

   if snowconvert_helpers.error_level > 0:
      LABEL_1()

   # Building the table.
   snowconvert_helpers.execute_sql_statement("""CREATE TABLE DEMO_DB.DEMO_DB.TABLE1
(
column1 BYTEINT,
column2 INTEGER,
column3 VARCHAR(10)
)""", con)
   LABEL_1()
except Exception as e:
   print(e)
finally:

   if con is not None:
      con.close()


   snowconvert_helpers.quit_application()