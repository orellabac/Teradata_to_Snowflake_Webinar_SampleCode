/* <sc-macro> DEMO_DB.macro_1 </sc-macro> */
-- Parameters vs column declarations in a Macro / Procedure.
CREATE OR REPLACE PROCEDURE DEMO_DB.DEMO_DB.macro_1 (COLUMN1 FLOAT, COLUMN2 FLOAT, COLUMN3 FLOAT)
   RETURNS STRING
   LANGUAGE JAVASCRIPT
   EXECUTE AS CALLER
   AS
   $$
var sql_stmt = `DELETE FROM DEMO_DB.DEMO_DB.table_macro1
WHERE column1 = ?
AND column2 = ?
AND column3 = ?`;
snowflake.createStatement({
   sqlText : sql_stmt,
   binds : [COLUMN1,COLUMN2,COLUMN3]
}).execute();
$$;