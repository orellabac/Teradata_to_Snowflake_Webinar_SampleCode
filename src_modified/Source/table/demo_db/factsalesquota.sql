/* <sc-table> DEMO_DB.FactSalesQuota </sc-table> */
CREATE TABLE DEMO_DB.FactSalesQuota ,NO FALLBACK ,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
    (
     SalesQuotaKey INTEGER NOT NULL,
     EmployeeKey INTEGER NOT NULL,
     DateKey INTEGER NOT NULL,
     CalendarYear SMALLINT NOT NULL,
     CalendarQuarter BYTEINT NOT NULL,
     SalesAmountQuota NUMBER(18,4) NOT NULL,
     "Date" DATE FORMAT 'yyyy-mm-dd', 
PRIMARY KEY ( SalesQuotaKey ))
PARTITION BY COLUMN (CALENDARYEAR);
;


