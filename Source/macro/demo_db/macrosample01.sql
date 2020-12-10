/* <sc-macro> DEMO_DB.MacroSample01 </sc-macro> */
REPLACE MACRO DEMO_DB.MacroSample01 (
parameter1 BYTEINT NOT NULL)
AS (
   SELECT * FROM DEMO_DB.TABLE1 WHERE column1 = parameter1;
);
