/* <sc-procedure> DEMO_DB.DYNAMIC_RESULT_SETS </sc-procedure> */
-- Multiple Result Sets
CREATE OR REPLACE PROCEDURE DEMO_DB.DEMO_DB.DYNAMIC_RESULT_SETS ()
   RETURNS STRING
   LANGUAGE JAVASCRIPT
   EXECUTE AS CALLER
   AS
   $$
 	var resultSetCounter = 0;
var tablelist = new Array;
{
   var SQL_CMD = ` `;
   var SQL_CMD_1 = ` `;
   var procname = `DEMO_DB.DEMO_DB.DYNAMIC_RESULT_SETS_`;
   var sql_command = `select current_session() || '_' || to_varchar(current_timestamp, 'yyyymmddhh24missss')`;
   var stmt = snowflake.createStatement({
      sqlText : sql_command
   });
   var res = stmt.execute();
   res.next();
   var sessionid = procname + res.getColumnValue(1);
   //------ MAIN --------
   SQL_CMD = `SELECT
*
FROM DEMO_DB.DEMO_DB.EMPLOYEE`;
   SQL_CMD_1 = `SELECT
*
FROM DEMO_DB.DEMO_DB.EMPLOYEE_PHONE_INFO`;
   //------ CURSORS --------
   var setname = SQL_CMD;
   var tablename = sessionid + `_` + resultSetCounter++;
   var sql_stmt = `CREATE TEMPORARY TABLE ${tablename} AS ${setname}`;
   tablelist.push(tablename);
   var RESULTSET = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   }).execute();
   var setname = SQL_CMD_1;
   var tablename = sessionid + `_` + resultSetCounter++;
   var sql_stmt = `CREATE TEMPORARY TABLE ${tablename} AS ${setname}`;
   tablelist.push(tablename);
   var RESULTSET1 = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   }).execute();
}
return tablelist;
 
$$;