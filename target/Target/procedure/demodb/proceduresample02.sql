/* <sc-procedure> DEMODB.ProcedureSample02 </sc-procedure> */
-- Abort Statements
CREATE OR REPLACE PROCEDURE DEMO_DB.DEMODB.ProcedureSample02 (PARAMETER1 FLOAT, SIZE FLOAT)
   RETURNS STRING
   LANGUAGE JAVASCRIPT
   EXECUTE AS CALLER
   AS
   $$
 	var BetweenFunc = function (expression,startExpr,endExpr) {
   return expression >= startExpr && expression <= endExpr;
};
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
{
   if (InFunc({
      sqlText : `(SELECT
column1
FROM DEMO_DB.PUBLIC.TABLE1)`,
      inArray : [],
      inValue : PARAMETER1
   })) {
      var sql_stmt = `ROLLBACK`;
      var stmt = snowflake.createStatement({
         sqlText : sql_stmt,
         binds : []
      });
      var res = stmt.execute();
      return `Parameter1 is invalid because it is already inside the column`;
   }
   if (InFunc({
      sqlText : '',
      inArray : [1,3,5,7,9,11],
      inValue : PARAMETER1
   })) {
      var sql_stmt = `ROLLBACK`;
      var stmt = snowflake.createStatement({
         sqlText : sql_stmt,
         binds : []
      });
      var res = stmt.execute();
      return `Parameter1 is invalid`;
   }
   if (BetweenFunc(PARAMETER1,1,100)) {
      var sql_stmt = `ROLLBACK`;
      var stmt = snowflake.createStatement({
         sqlText : sql_stmt,
         binds : []
      });
      var res = stmt.execute();
      return `Parameter1 is invalid because ....`;
   }
   var sql_stmt = `SELECT
*
FROM DEMO_DB.DEMO_DB.TABLE1 WHERE column1 = ?`;
   snowflake.createStatement({
      sqlText : sql_stmt,
      binds : [PARAMETER1]
   }).execute();
}
 
$$;