/* <sc-procedure> DEMODB.ProcedureSample01 </sc-procedure> */
-- Error Handlers
REPLACE PROCEDURE DEMODB.ProcedureSample01(parameter1 INTEGER, SIZE INTEGER)
BEGIN
DECLARE  myLocalVariable1 INTEGER;
DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
             CALL LogErrorMacro('there was an error...');
        END;

SET INTERVAL_SIZE = CASE 
              WHEN SIZE  IS NULL THEN 5 
              WHEN SIZE  IS NOT NULL THEN 6 
              ELSE SIZE 
            END;

IF ((CASE 
        WHEN SIZE  IS NULL THEN 200
        WHEN SIZE  IS NOT NULL THEN 1 
        ELSE SIZE 
    END) > 100) THEN
        ABORT;
END IF;

SELECT * FROM DEMO_DB.TABLE1 WHERE column1 = :parameter1;

END;


