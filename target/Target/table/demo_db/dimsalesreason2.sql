/* <sc-table> DEMO_DB.DimSalesReason2 </sc-table> */
/**** WARNING: SET TABLE FUNCTIONALITY NOT SUPPORTED ****/
CREATE TABLE DEMO_DB.DEMO_DB.DimSalesReason2
(
SalesReasonKey INTEGER NOT NULL,
SalesReasonAlternateKey INTEGER NOT NULL,
SalesReasonName VARCHAR(50) COLLATE 'en-ci' NOT NULL,
SalesReasonReasonType VARCHAR(50) COLLATE 'en-ci' NOT NULL,
PRIMARY KEY ( SalesReasonKey ))
;