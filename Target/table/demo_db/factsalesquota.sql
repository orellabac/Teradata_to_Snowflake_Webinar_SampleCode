/* <sc-table> DEMO_DB.FactSalesQuota </sc-table> */
CREATE TABLE DEMO_DB.DEMO_DB.FactSalesQuota
(
SalesQuotaKey INTEGER NOT NULL,
EmployeeKey INTEGER NOT NULL,
DateKey INTEGER NOT NULL,
CalendarYear SMALLINT NOT NULL,
CalendarQuarter BYTEINT NOT NULL,
SalesAmountQuota NUMBER(18,4) NOT NULL,
"Date" DATE,
PRIMARY KEY ( SalesQuotaKey ))
/**** WARNING: PERFORMANCE REVIEW - CLUSTER BY ****/
/*CLUSTER BY(CALENDARYEAR)*/;