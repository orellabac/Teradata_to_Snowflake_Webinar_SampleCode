/* <sc-procedure> DEMO_DB.CURSORS </sc-procedure> */
-- DYNAMIC SQL, ACTIVITY_COUNT, CURSOR FOR, IF/THEN, Variable
CREATE OR REPLACE PROCEDURE DEMO_DB."DEMO_DB"."CURSORS" ("V_TARGETTABLE" STRING)
   RETURNS STRING
   LANGUAGE JAVASCRIPT
   EXECUTE AS CALLER
   AS
   $$
 	var ACTIVITY_COUNT = 0;
var resultSetCounter = 0;
var tablelist = new Array;
{
   var V_EMP_ID;
   var V_EMP_NAME;
   var V_MYSQLCODE;
   var V_MYSQLSTATE;
   var V_CNT;
   var V_KEEPCOUNT;
   var V_ACT_CNT;
   var sql_stmt = `SELECT
'EMPLOYEE' as id_el,
EMP_NAME as id_la
FROM DEMO_DB.DEMO_DB.EMPLOYEE;`;
   var CUR1 = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   }).execute();
   var sql_stmt = `SELECT COUNT(*) FROM DEMO_DB.DEMO_DB.ACT_COUNT;`;
   var stmt = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   });
   var res = stmt.execute();
   res.next();
   var ACTIVITY_COUNT = res.getColumnValue(1);
   var sql_stmt = `TRUNCATE TABLE DEMO_DB.DEMO_DB.ACT_COUNT;`;
   var stmt = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   }).execute();
   V_ACT_CNT = ACTIVITY_COUNT;
   var sql_stmt = `INSERT INTO DEMO_DB.DEMO_DB.ACT_COUNT VALUES (${V_ACT_CNT},'FIRST')`;
   var stmt = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   }).execute();
   var sql_stmt = `DROP TABLE DEMO_DB.DEMO_DB.EMPLOYEE_DUPE;`;
   var stmt = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   }).execute();
   var sql_stmt = `CREATE TABLE DEMO_DB.DEMO_DB.EMPLOYEE_DUPE AS (SELECT
*
FROM DEMO_DB.DEMO_DB.EMPLOYEE);`;
   var stmt = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   }).execute();
   var sql_stmt = `SELECT COUNT(*) FROM ;`;
   var stmt = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   });
   var res = stmt.execute();
   res.next();
   var ACTIVITY_COUNT = res.getColumnValue(1);
   var sql_stmt = `UPDATE DEMO_DB.DEMO_DB.EMPLOYEE_DUPE SET EMP_NAME = 'BOB' WHERE EMP_ID <=2;`;
   var stmt = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   }).execute();
   V_ACT_CNT = V_ACT_CNT + ACTIVITY_COUNT;
   var sql_stmt = `INSERT INTO DEMO_DB.DEMO_DB.ACT_COUNT VALUES (${V_ACT_CNT},'SECOND')`;
   var stmt = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   }).execute();
   var sql_stmt = `SELECT COUNT(*) FROM ;`;
   var stmt = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   });
   var res = stmt.execute();
   res.next();
   var ACTIVITY_COUNT = res.getColumnValue(1);
   var sql_stmt = `DELETE FROM DEMO_DB.DEMO_DB.EMPLOYEE_DUPE WHERE EMP_ID = 4;`;
   var stmt = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   }).execute();
   V_ACT_CNT = V_ACT_CNT + ACTIVITY_COUNT;
   var sql_stmt = `INSERT INTO DEMO_DB.DEMO_DB.ACT_COUNT VALUES (${V_ACT_CNT},'THIRD')`;
   var stmt = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   }).execute();
   //   ;
   V_KEEPCOUNT = V_ACT_CNT - 1;
   V_CNT = 1;
   while ( V_CNT < V_KEEPCOUNT ) {
      CUR1.next();
      v_emp_id = CUR1.getColumnValue(1);
      v_emp_name = CUR1.getColumnValue(2);
      var sql_stmt = `ALTER TABLE DEMO_DB.PUBLIC.${v_TargetTable} ADD AT_${V_EMP_NAME} VARCHAR(85)`;
      var stmt = snowflake.createStatement({
         sqlText : sql_stmt,
         binds : []
      }).execute();
      var sql_stmt = `UPDATE DEMO_DB.PUBLIC.${v_TargetTable} SET AT_${V_EMP_NAME} = (SELECT
EMP_NAME
FROM DEMO_DB.DEMO_DB.EMPLOYEE WHERE EMP_ID = ${V_CNT})`;
      var stmt = snowflake.createStatement({
         sqlText : sql_stmt,
         binds : []
      }).execute();
      V_CNT = V_CNT + 1;
   }
}
return tablelist;
 
$$;