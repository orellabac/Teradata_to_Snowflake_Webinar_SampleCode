/* <sc-table> DEMO_DB.TABLE1  </sc-table> */
CREATE SET TABLE DEMO_DB.TABLE_1 ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
         column1 INTERVAL HOUR(2),
         column2 INTEGER,
         column3 VARCHAR(10)
     );


