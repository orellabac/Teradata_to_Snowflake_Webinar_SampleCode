/* <sc-procedure> DEMODB.ProcedureSample02 </sc-procedure> */
-- Abort Statements
REPLACE PROCEDURE DEMODB.ProcedureSample02(parameter1 INTEGER, SIZE INTEGER)
BEGIN

ABORT 'Parameter1 is invalid because it is already inside the column' 
WHERE parameter1 IN (SELECT column1 FROM TABLE1);

ABORT 'Parameter1 is invalid' 
WHERE parameter1 IN (1,3,5,7,9,11);


ABORT 'Parameter1 is invalid because ....' 
WHERE parameter1 BETWEEN 1 AND 100;

SELECT * FROM DEMO_DB.TABLE1 WHERE column1 = :parameter1;

END;


