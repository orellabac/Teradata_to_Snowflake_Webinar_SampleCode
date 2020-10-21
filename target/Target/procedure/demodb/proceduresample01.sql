/* <sc-procedure> DEMODB.ProcedureSample01 </sc-procedure> */
-- Error Handlers
CREATE OR REPLACE PROCEDURE DEMO_DB.DEMODB.ProcedureSample01 (PARAMETER1 FLOAT, SIZE FLOAT)
   RETURNS STRING
   LANGUAGE JAVASCRIPT
   EXECUTE AS CALLER
   AS
   $$
 	{
   var MYLOCALVARIABLE1;
   var _RS, ROW_COUNT, _ROWS, MESSAGE_TEXT, SQLCODE, SQLSTATE, ERROR_HANDLERS, ACTIVITY_COUNT, INTO;
   var EXEC = function (stmt,binds,noCatch) {
      try {
         var fixBind = function (arg) {
            arg = arg == undefined ? null : arg instanceof Date ? arg.toISOString() : arg;
            return arg;
         };
         binds = binds ? binds.map(fixBind) : binds;
         _RS = snowflake.createStatement({
               sqlText : stmt,
               binds : binds
            });
         _ROWS = _RS.execute();
         ROW_COUNT = _RS.getRowCount();
         ACTIVITY_COUNT = _RS.getNumRowsAffected();
         if (INTO) return {
            INTO : function () {
               return INTO();
            }
         };
      } catch(error) {
         MESSAGE_TEXT = error.message;
         SQLCODE = error.code;
         SQLSTATE = error.state;
         if (!noCatch && ERROR_HANDLERS) ERROR_HANDLERS(error);
      }
   };
   //--------------------------------------------------
   var exit_handler_1 = function (error) {
      {
         EXEC(`CALL DEMO_DB.PUBLIC.LogErrorMacro('there was an error...')`,[]);
      }
      throw error;
   };
   var ERROR_HANDLERS = function (error) {
      switch(error.sqlstate) {
         //Conversion Warning - handlers for the switch default (SQLWARNING/SQLEXCEPTION/NOT FOUND) can be the following
         default:exit_handler_1(error);
      }
   };
   INTERVAL_SIZE = ((SIZE == null && 5) || (SIZE != null || 6) || SIZE);
   if (((SIZE == null) && 200) || (SIZE != null && 1) || SIZE)) > 100) {
      EXEC(`ROLLBACK`);
      return null;
   }
   EXEC(`SELECT
*
FROM DEMO_DB.DEMO_DB.TABLE1 WHERE column1 = ?`,[PARAMETER1]);
}
 
$$;