/* <sc-procedure> DEMO_DB.CREATE_INSERTS </sc-procedure> */
-- WHILE LOOP, DYNAMIC SQL, SYSTEM TABLE QUERIES, UPDATE RESTRUCTURE, IF/THEN 
CREATE OR REPLACE PROCEDURE DEMO_DB.DEMO_DB.CREATE_INSERTS (DB_NAME STRING, TBL_NAME STRING)
   RETURNS STRING
   LANGUAGE JAVASCRIPT
   EXECUTE AS CALLER
   AS
   $$
 	var InFunc = function (objectParam) {
   var result = objectParam['inArray'];
   if (objectParam['sqlText'] != '') {
      var res = snowflake.createStatement({
         sqlText : objectParam['sqlText']
      }).execute();
      while ( res.next() ) {
         result.push(res.getColumnValue(1));
      }
   }
   return result.includes(objectParam['inValue']);
};
var resultSetCounter = 0;
var tablelist = new Array;
{
   var COL_NAME;
   var SUFFIX;
   var PREFIX;
   var MIDDLE;
   var PRE_STMT;
   var COL_TYPE;
   var PREV_COL_TYPE;
   var COL_LIST;
   var SQL_STMT;
   var SQL_CMD;
   var COL_COUNT;
   var NUM_COLS;
   var COL_LEN;
   var BIG_OBJ;
   var procname = `DEMO_DB.DEMO_DB.CREATE_INSERTS_`;
   var sql_command = `select current_session() || '_' || to_varchar(current_timestamp, 'yyyymmddhh24missss')`;
   var stmt = snowflake.createStatement({
      sqlText : sql_command
   });
   var res = stmt.execute();
   res.next();
   var sessionid = procname + res.getColumnValue(1);
   var sql_stmt = `SELECT
COUNT(COLUMN_NAME)
FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_CATALOG = '${DB_NAME}' AND TABLE_NAME = '${TBL_NAME}' GROUP BY TABLE_CATALOG, TABLE_NAME ORDER BY TABLE_NAME;`;
   var CUR2 = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   }).execute();
   var sql_stmt = `SELECT
TRIM(UPPER(COLUMN_NAME))
FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_CATALOG = '${DB_NAME}' AND TABLE_NAME = '${TBL_NAME}' ORDER BY TABLE_CATALOG, TABLE_NAME, COLUMN_NAME;`;
   var CUR3 = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   }).execute();
   var sql_stmt = `SELECT
CASE WHEN DATA_TYPE IN ('I', 'DA') THEN 20 ELSE CHARACTER_MAXIMUM_LENGTH END
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_CATALOG = '${DB_NAME}' AND TABLE_NAME = '${TBL_NAME}' ORDER BY TABLE_CATALOG, TABLE_NAME, COLUMN_NAME;`;
   var CUR4 = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   }).execute();
   var sql_stmt = `SELECT
DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_CATALOG = '${DB_NAME}' AND TABLE_NAME = '${TBL_NAME}'
ORDER BY TABLE_CATALOG, TABLE_NAME, COLUMN_NAME;`;
   var CUR5 = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   }).execute();
   var sql_stmt = `UPDATE DEMO_DB.DEMO_DB.DIMACCOUNT AS MOD_TBLUPDATE SET MOD_TBLUPDATE.ACCOUNTTYPE = 'Revenue' FROM DEMO_DB.DEMO_DB.DIMACCOUNT MOD_TBL WHERE MOD_TBL.ACCOUNTKEY = 61;`;
   var stmt = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   }).execute();
   var sql_stmt = `UPDATE DEMO_DB.DEMO_DB.DIMACCOUNT AS MOD_TBLUPDATE SET MOD_TBLUPDATE.ACCOUNTTYPE = 'Expenditures' FROM DEMO_DB.DEMO_DB.DIMACCOUNT MOD_TBL WHERE MOD_TBL.ACCOUNTKEY = 61;`;
   var stmt = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   }).execute();
   //   ;
   //   ;
   //   ;
   //   ;
   CUR2.next();
   NUM_COLS = CUR2.getColumnValue(1);
   CUR3.next();
   COL_NAME = CUR3.getColumnValue(1);
   CUR4.next();
   COL_LEN = CUR4.getColumnValue(1);
   CUR5.next();
   COL_TYPE = CUR5.getColumnValue(1);
   COL_COUNT = 2;
   COL_LIST = ``;
   SQL_STMT = ``;
   BIG_OBJ = 1;
   while ( BIG_OBJ != 0 ) {
      if (COL_TYPE == `BO`) {
         COL_COUNT = COL_COUNT + 1;
         CUR3.next();
         COL_NAME = CUR3.getColumnValue(1);
         CUR4.next();
         COL_LEN = CUR4.getColumnValue(1);
         PREV_COL_TYPE = COL_TYPE;
         CUR5.next();
         COL_TYPE = CUR5.getColumnValue(1);
      } else {
         BIG_OBJ = 0;
      }
   }
   if (InFunc({
      sqlText : '',
      inArray : [`CLASS`,`DATE`,`CLASS`],
      inValue : COL_NAME
   })) {
      //Conversion warning - Procedure - This statement may be a dynamic sql that could not be recognized and converted 
      COL_NAME = `"${COL_NAME}"`;
   }
   //Conversion warning - Procedure - This statement may be a dynamic sql that could not be recognized and converted 
   PREFIX = `SELECT ''INSERT INTO ${DB_NAME}.${TBL_NAME}(${COL_NAME}`;
   //Conversion warning - Procedure - This statement may be a dynamic sql that could not be recognized and converted 
   PRE_STMT = `CAST(OREPLACE(TRIM(COALESCE(CAST("${COL_NAME}" AS VARCHAR(${COL_LEN})), )), '''''''', '''''''''''') AS VARCHAR(${COL_LEN}5))`;
   while ( COL_COUNT <= NUM_COLS ) {
      CUR3.next();
      COL_NAME = CUR3.getColumnValue(1);
      if (InFunc({
         sqlText : '',
         inArray : [`CLASS`,`DATE`,`CLASS`],
         inValue : COL_NAME
      })) {
         //Conversion warning - Procedure - This statement may be a dynamic sql that could not be recognized and converted 
         COL_NAME = `"${COL_NAME}"`;
      }
      CUR4.next();
      COL_LEN = CUR4.getColumnValue(1);
      PREV_COL_TYPE = COL_TYPE;
      CUR5.next();
      COL_TYPE = CUR5.getColumnValue(1);
      if (COL_TYPE != `BO`) {
         //Conversion warning - Procedure - This statement may be a dynamic sql that could not be recognized and converted 
         COL_LIST = `${COL_LIST}, ${COL_NAME}`;
         if (PREV_COL_TYPE == `PD`) {
            //Conversion warning - Procedure - This statement may be a dynamic sql that could not be recognized and converted 
            SQL_STMT = `${SQL_STMT} || '', `;
         } else {
            //Conversion warning - Procedure - This statement may be a dynamic sql that could not be recognized and converted 
            SQL_STMT = `${SQL_STMT} || '''''', `;
         }
         if (COL_TYPE == `PD`) {
            //Conversion warning - Procedure - This statement may be a dynamic sql that could not be recognized and converted 
            SQL_STMT = `${SQL_STMT}PERIOD (DATE'''''' || CAST(BEGIN(${COL_NAME}) AS VARCHAR(10)) || '''''', DATE '''''' || CAST(END(${COL_NAME})AS VARCHAR(10)) || '''''')`;
         } else {
            //Conversion warning - Procedure - This statement may be a dynamic sql that could not be recognized and converted 
            SQL_STMT = `${SQL_STMT} || CAST(OREPLACE(TRIM(COALESCE(CAST("${COL_NAME}" AS VARCHAR(${COL_LEN})), )), '''''''', '''''''''''') AS VARCHAR(${COL_LEN}5))`;
         }
      }
      COL_COUNT = COL_COUNT + 1;
   }
   //Conversion warning - Procedure - This statement may be a dynamic sql that could not be recognized and converted 
   SUFFIX = ` || '''''');'' "--"  FROM ${DB_NAME}.${TBL_NAME}`;
   MIDDLE = `) VALUES(''' || `;
   //Conversion warning - Procedure - This statement may be a dynamic sql that could not be recognized and converted 
   SQL_CMD = `${PREFIX}${COL_LIST}${MIDDLE}${PRE_STMT}${SQL_STMT}${SUFFIX}`;
   //		SET SSQQLL = SQL_CMD;	
   var setname = SQL_CMD;
   var tablename = sessionid + `_` + resultSetCounter++;
   var sql_stmt = `CREATE TEMPORARY TABLE ${tablename} AS ${setname}`;
   tablelist.push(tablename);
   var RESULTSET = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   }).execute();
}
return tablelist;
 
$$;