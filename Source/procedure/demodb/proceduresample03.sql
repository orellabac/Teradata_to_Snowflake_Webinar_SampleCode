/* <sc-procedure> DEMODB.ProcedureSample03 </sc-procedure> */
-- For Cursor Loop
REPLACE PROCEDURE DEMODB.ProcedureSample03(parameter1 INTEGER, SIZE INTEGER)
BEGIN

FOR fUsgClass AS cUsgClass CURSOR FOR
    SELECT 
        column1,
        column2,
        column3
    FROM DEMO_DB.TABLE1
DO
    BEGIN
        DECLARE columnByteInt INTEGER;
        DECLARE columnInteger INTEGER;
        DECLARE columnVarchar varchar(1000);
        SET columnByteInt = fUsgClass.column1 + 1;
        SET columnInteger = fUsgClass.column2 + 1;
        SET columnVarchar = fUsgClass.column3 || 'HELLO WORLD';
        INSERT INTO TABLE1 VALUES(:columnByteInt, :columnInteger, :columnVarchar);
    END;
END FOR;

END;


