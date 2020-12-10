/* <sc-procedure> DEMODB.ProcedureSample03 </sc-procedure> */
-- For Cursor Loop
CREATE OR REPLACE PROCEDURE DEMO_DB.DEMODB.ProcedureSample03 (PARAMETER1 FLOAT, SIZE FLOAT)
   RETURNS STRING
   LANGUAGE JAVASCRIPT
   EXECUTE AS CALLER
   AS
   $$
 	{
   var sql_stmt = `SELECT
column1,
column2,
column3
FROM DEMO_DB.DEMO_DB.TABLE1`;
   var stmt = snowflake.createStatement({
      sqlText : sql_stmt,
      binds : []
   });
   var res = stmt.execute();
   while ( res.next() ) {
      var fUsgClass = {
         column1 : res.getColumnValue(1),
         column2 : res.getColumnValue(2),
         column3 : res.getColumnValue(3)
      };
      {
         var COLUMNBYTEINT;
         var COLUMNINTEGER;
         var COLUMNVARCHAR;
         COLUMNBYTEINT = fUsgClass.column1 + 1;
         COLUMNINTEGER = fUsgClass.column2 + 1;
         //Conversion warning - Procedure - This statement may be a dynamic sql that could not be recognized and converted 
         COLUMNVARCHAR = `${fUsgClass}${column3}HELLO WORLD`;
         var sql_stmt = `INSERT INTO DEMO_DB.PUBLIC.TABLE1 VALUES (?, ?, ?)`;
         snowflake.createStatement({
            sqlText : sql_stmt,
            binds : [COLUMNBYTEINT,COLUMNINTEGER,COLUMNVARCHAR]
         }).execute();
      }
   }
}
 
$$;