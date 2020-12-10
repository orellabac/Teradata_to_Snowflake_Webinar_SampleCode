/* <sc-macro> DEMO_DB.macro_1 </sc-macro> */
-- Parameters vs column declarations in a Macro / Procedure.
REPLACE MACRO DEMO_DB.macro_1 (
  column1 BYTEINT NOT NULL,
  column2 SMALLINT NOT NULL,
  column3 SMALLINT NOT NULL)
AS
(
  DELETE FROM DEMO_DB.table_macro1
  WHERE column1 = :column1
  AND column2 = :column2
  AND column3 = :column3;
);

