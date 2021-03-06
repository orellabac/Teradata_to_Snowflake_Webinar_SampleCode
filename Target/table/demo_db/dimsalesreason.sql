/* <sc-table> DEMO_DB.DimSalesReason </sc-table> */
/**** WARNING: SET TABLE FUNCTIONALITY NOT SUPPORTED ****/
CREATE TABLE DEMO_DB.DEMO_DB.DimSalesReason
(
EmployeeKey INTEGER NOT NULL,
ParentEmployeeKey INTEGER,
EmployeeNationalIDAlternateKey VARCHAR(15) COLLATE 'en-ci',
FirstName VARCHAR(50) COLLATE 'en-ci' NOT NULL,
NameStyle BYTEINT
-- *** WARNING: THE FOLLOWING CHECK CONSTRAINT WAS COMMENTED OUT ***
--                  CHECK ( NameStyle  IN (0 ,1 ) )
                                                  NOT NULL,
"Title" VARCHAR(50) COLLATE 'en-ci',
HireDate DATE,
PayFrequency BYTEINT,
BaseRate NUMBER(18,4),
VacationHours SMALLINT,
EmployeePhoto BINARY /**** WARNING: Column converted from BLOB data type ****/,
Weight NUMBER(38, 19),
SalesOrderLineNumber SMALLINT NOT NULL,
SalesReasonKey INTEGER NOT NULL,
EmailAddress VARCHAR(50) COLLATE 'en-ci',
EnglishEducation VARCHAR(40) COLLATE 'en-ci',
EMPLOYMENT_PERIOD VARCHAR(24) COMMENT 'PERIOD(DATE)' /**** WARNING: PERIOD DATA TYPE "PERIOD(DATE)" CONVERTED TO VARCHAR ****/,
INTERVAL_YEAR_TYPE VARCHAR(20) COMMENT 'INTERVAL YEAR(2)' /**** WARNING: INTERVAL DATA TYPE "INTERVAL YEAR(2)" CONVERTED TO VARCHAR ****/,
INTERVAL_MONTH_TYPE VARCHAR(20) COMMENT 'INTERVAL MONTH(2)' /**** WARNING: INTERVAL DATA TYPE "INTERVAL MONTH(2)" CONVERTED TO VARCHAR ****/,
SALARY_AMOUNT DECIMAL(10,2) NOT NULL,
TS TIMESTAMP(6),

PRIMARY KEY ( EmployeeKey ))
;