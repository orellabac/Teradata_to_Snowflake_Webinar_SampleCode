/* <sc-macro> DEMO_DB.MacroSample01 </sc-macro> */
CREATE OR REPLACE PROCEDURE DEMO_DB.DEMO_DB.MacroSample01 (PARAMETER1 FLOAT)
   RETURNS STRING
   LANGUAGE JAVASCRIPT
   EXECUTE AS CALLER
   AS
   $$
 	var procname = `DEMO_DB.DEMO_DB.MacroSample01`;
var temptable_prefix, tablelist = [];
var INSERT_TEMP = function (setname,parameters) {
   if (!temptable_prefix) {
      var sql_stmt = `select current_session() || '_' || to_varchar(current_timestamp, 'yyyymmddhh24missss')`;
      var _RS = snowflake.createStatement({
         sqlText : sql_stmt,
         binds : []
      }).execute();
      temptable_prefix = _RS.next() && (procname + '_TEMP_' + _RS.getColumnValue(1) + '_');
   }
   var tablename = temptable_prefix + tablelist.length;
   tablelist.push(tablename);
   var sql_stmt = `CREATE TEMPORARY TABLE ${tablename} AS ${setname}`;
   snowflake.execute({
      sqlText : sql_stmt,
      binds : parameters
   });
   return tablename;
};
INSERT_TEMP(`SELECT
*
FROM DEMO_DB.DEMO_DB.TABLE1 WHERE column1 = parameter1`,[PARAMETER1]);
return tablelist;
 
$$;