/* <sc-procedure> DEMO_DB.CURSORS </sc-procedure> */
-- DYNAMIC SQL, ACTIVITY_COUNT, CURSOR FOR, IF/THEN, Variable
REPLACE PROCEDURE "DEMO_DB"."CURSORS"  
(
  IN "v_TargetTable" VARCHAR(300) CHARACTER SET LATIN
  )
DYNAMIC RESULT SETS 1
	
BEGIN
    
	DECLARE v_emp_id VARCHAR(50);
    DECLARE v_emp_name VARCHAR(50);
    DECLARE v_MySQLCode INTEGER;
	DECLARE v_MySQLState CHAR(5);
	DECLARE v_CNT, v_KEEPCOUNT INTEGER;
	DECLARE v_ACT_CNT INTEGER;
	DECLARE CUR1 CURSOR FOR SELECT 'EMPLOYEE' as id_el, EMP_NAME as id_la FROM DEMO_DB.EMPLOYEE;
	
	CALL DBC.SYSEXECSQL('DELETE DEMO_DB.ACT_COUNT ALL;');
	
	SET v_ACT_CNT = ACTIVITY_COUNT;
	CALL DBC.SYSEXECSQL('INSERT INTO DEMO_DB.ACT_COUNT(' || v_ACT_CNT || ',''FIRST'');');
		
	CALL DBC.SYSEXECSQL('DROP TABLE DEMO_DB.EMPLOYEE_DUPE');
	CALL DBC.SYSEXECSQL('CREATE MULTISET TABLE DEMO_DB.EMPLOYEE_DUPE AS (SELECT * FROM DEMO_DB.EMPLOYEE) WITH DATA');
	
	CALL DBC.SYSEXECSQL('UPDATE DEMO_DB.EMPLOYEE_DUPE SET EMP_NAME = ''BOB'' WHERE EMP_ID <=2');
	
	SET v_ACT_CNT = v_ACT_CNT + ACTIVITY_COUNT;
	CALL DBC.SYSEXECSQL('INSERT INTO DEMO_DB.ACT_COUNT(' || v_ACT_CNT || ',''SECOND'');');	
	
	CALL DBC.SYSEXECSQL('DELETE FROM DEMO_DB.EMPLOYEE_DUPE WHERE EMP_ID = 4');

	SET v_ACT_CNT = v_ACT_CNT + ACTIVITY_COUNT;
	CALL DBC.SYSEXECSQL('INSERT INTO DEMO_DB.ACT_COUNT(' || v_ACT_CNT || ',''THIRD'');');

	OPEN CUR1;
	
	SET v_KEEPCOUNT = v_ACT_CNT - 1;
	SET v_CNT = 1;

	WHILE (v_CNT < v_KEEPCOUNT) DO
		FETCH CUR1 INTO v_emp_id, v_emp_name;
		CALL DBC.SYSEXECSQL('ALTER TABLE ' || v_TargetTable || ' ADD AT_' || v_emp_name || ' VARCHAR(85) CHARACTER SET LATIN CASESPECIFIC;');
		CALL DBC.SYSEXECSQL('UPDATE ' || v_TargetTable || ' SET AT_' || v_emp_name || ' = (SELECT EMP_NAME FROM DEMO_DB.EMPLOYEE WHERE EMP_ID = ' || v_CNT || ');');
		SET v_CNT = v_CNT + 1;
	END WHILE;			
END;

CALL DEMO_DB.CURSORS('EMPLOYEE_DUPE');

SELECT * FROM DEMO_DB.EMPLOYEE_DUPE;

