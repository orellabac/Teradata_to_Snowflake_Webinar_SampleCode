CREATE OR REPLACE FUNCTION DEMO_DB.PUBLIC.TIMESTAMP_TO_PERIOD_UDF(INPUT_VALUE TIMESTAMP)
RETURNS VARCHAR 
AS
'
''('' || INPUT_VALUE || '', '' || TO_VARCHAR(TIMESTAMPADD(''SECOND'', .00001, TO_TIMESTAMP(INPUT_VALUE))) || '')''
';

CREATE OR REPLACE FUNCTION DEMO_DB.PUBLIC.DATE_TO_PERIOD_UDF(INPUT_VALUE DATE)
RETURNS VARCHAR 
AS
'
''('' || INPUT_VALUE || '', '' || TO_VARCHAR(TO_DATE(INPUT_VALUE) + 1) || '')''
';


CREATE OR REPLACE FUNCTION DEMO_DB.PUBLIC.INTERVAL2MONTHS_UDF
(INPUT_VALUE VARCHAR())
RETURNS INTEGER 
AS
'
CASE WHEN SUBSTR(INPUT_VALUE,1,1) = ''-'' THEN
   12 * CAST(SUBSTR(INPUT_VALUE,1 , POSITION(''-'', INPUT_VALUE,2)-1) AS INTEGER)
   - CAST(SUBSTR(INPUT_VALUE,POSITION(''-'', INPUT_VALUE)+1) AS INTEGER)
ELSE
   12 * CAST(SUBSTR(INPUT_VALUE,1 , POSITION(''-'', INPUT_VALUE,2)-1) AS INTEGER)
   + CAST(SUBSTR(INPUT_VALUE,POSITION(''-'', INPUT_VALUE)+1) AS INTEGER)
END
';

CREATE OR REPLACE FUNCTION DEMO_DB.PUBLIC.PERIOD_BEGIN_UDF(PERIOD_VAL VARCHAR(22))
RETURNS TIMESTAMP
AS
' CAST(SUBSTR(PERIOD_VAL,1, POSITION(''*'',PERIOD_VAL)-1) AS TIMESTAMP) '     
;

CREATE OR REPLACE FUNCTION DEMO_DB.PUBLIC.PERIOD_END_UDF(PERIOD_VAL VARCHAR(22))
RETURNS TIMESTAMP
AS
' CAST(SUBSTR(PERIOD_VAL,POSITION(''*'',PERIOD_VAL)+1) AS TIMESTAMP) '     
;

CREATE OR REPLACE FUNCTION DEMO_DB.PUBLIC.PERIOD_OVERLAPS_UDF(PERIOD_1 VARCHAR(22), PERIOD_2 VARCHAR(22))
RETURNS BOOLEAN 
AS
' CASE WHEN 
    (PERIOD_END_UDF(PERIOD_1) >= PERIOD_BEGIN_UDF(PERIOD_2) AND
     PERIOD_BEGIN_UDF(PERIOD_1)  < PERIOD_END_UDF(PERIOD_2))
    OR
    (PERIOD_BEGIN_UDF(PERIOD_1) >= PERIOD_BEGIN_UDF(PERIOD_2)AND
     PERIOD_BEGIN_UDF(PERIOD_1) < PERIOD_END_UDF(PERIOD_2)
    )
THEN
    TRUE
ELSE
    FALSE
END '
;

CREATE OR REPLACE FUNCTION DEMO_DB.PUBLIC.INTERVAL2SECONDS_UDF
(INPUT_PART VARCHAR(30), INPUT_VALUE VARCHAR())
RETURNS DECIMAL(20,6) 
AS
'
CASE WHEN SUBSTR(INPUT_VALUE,1,1) = ''-'' THEN
   DECODE(INPUT_PART,
           ''DAY'',              86400 * INPUT_VALUE, 
           ''DAY TO HOUR'',      86400 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION('' '', INPUT_VALUE)-1) AS DECIMAL(10,0)) 
                               - 3600 * CAST(SUBSTR(INPUT_VALUE, POSITION('' '', INPUT_VALUE)+1) AS DECIMAL(10,0)),
           ''DAY TO MINUTE'',    86400 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION('' '', INPUT_VALUE)-1) AS INTEGER) 
                               - 3600 * CAST(SUBSTR(INPUT_VALUE, POSITION('' '', INPUT_VALUE)+1, POSITION('':'', INPUT_VALUE)-POSITION('' '', INPUT_VALUE)-1) AS INTEGER) 
                               - 60 * CAST(SUBSTR(INPUT_VALUE, POSITION('':'', INPUT_VALUE)+1) AS INTEGER),
           ''DAY TO SECOND'',    86400 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION('' '', INPUT_VALUE)-1) AS INTEGER) 
                               - 3600 * CAST(SUBSTR(INPUT_VALUE, POSITION('' '', INPUT_VALUE)+1, POSITION('':'', INPUT_VALUE)-POSITION('' '', INPUT_VALUE)-1) AS INTEGER)
                               - 60 * CAST(SUBSTR(INPUT_VALUE, POSITION('':'', INPUT_VALUE)+1, POSITION('':'', INPUT_VALUE, POSITION('':'', INPUT_VALUE)+1) - POSITION('':'', INPUT_VALUE) - 1) AS INTEGER)
                               - CAST(SUBSTR(INPUT_VALUE,POSITION('':'', INPUT_VALUE, POSITION('':'', INPUT_VALUE)+1)+1) AS DECIMAL(10,6)),
           ''HOUR'',             3600 * INPUT_VALUE, 
           ''HOUR TO MINUTE'',   3600 * CAST(SUBSTR(INPUT_VALUE,1 , POSITION('':'', INPUT_VALUE)-1) AS INTEGER)
                               - 60 * CAST(SUBSTR(INPUT_VALUE,POSITION('':'', INPUT_VALUE)+1) AS INTEGER),
           ''HOUR TO SECOND'',   3600 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION('':'', INPUT_VALUE)-POSITION('' '', INPUT_VALUE)-1) AS INTEGER)
                               - 60 * CAST(SUBSTR(INPUT_VALUE, POSITION('':'', INPUT_VALUE)+1, POSITION('':'', INPUT_VALUE, POSITION('':'', INPUT_VALUE)+1) - POSITION('':'', INPUT_VALUE) - 1) AS INTEGER)
                               - CAST(SUBSTR(INPUT_VALUE,POSITION('':'', INPUT_VALUE, POSITION('':'', INPUT_VALUE)+1)+1) AS DECIMAL(10,6)),  
           ''MINUTE'',           60 * INPUT_VALUE,     
           ''MINUTE TO SECOND'', 60 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION('':'', INPUT_VALUE)-POSITION('' '', INPUT_VALUE)-1) AS INTEGER)
                               - CAST(SUBSTR(INPUT_VALUE, POSITION('':'', INPUT_VALUE)+1) AS DECIMAL(10,6)),
           ''SECOND'',           INPUT_VALUE                                    
            )
ELSE
   DECODE(INPUT_PART,
           ''DAY'',              86400 * INPUT_VALUE, 
           ''DAY TO HOUR'',      86400 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION('' '', INPUT_VALUE)-1) AS INTEGER) 
                               + 3600 * CAST(SUBSTR(INPUT_VALUE, POSITION('' '', INPUT_VALUE)+1) AS INTEGER),
           ''DAY TO MINUTE'',    86400 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION('' '', INPUT_VALUE)-1) AS INTEGER) 
                               + 3600 * CAST(SUBSTR(INPUT_VALUE, POSITION('' '', INPUT_VALUE)+1, POSITION('':'', INPUT_VALUE)-POSITION('' '', INPUT_VALUE)-1) AS INTEGER) 
                               + 60 * CAST(SUBSTR(INPUT_VALUE, POSITION('':'', INPUT_VALUE)+1) AS INTEGER),
           ''DAY TO SECOND'',    86400 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION('' '', INPUT_VALUE)-1) AS INTEGER) 
                               + 3600 * CAST(SUBSTR(INPUT_VALUE, POSITION('' '', INPUT_VALUE)+1, POSITION('':'', INPUT_VALUE)-POSITION('' '', INPUT_VALUE)-1) AS INTEGER)
                               + 60 * CAST(SUBSTR(INPUT_VALUE, POSITION('':'', INPUT_VALUE)+1, POSITION('':'', INPUT_VALUE, POSITION('':'', INPUT_VALUE)+1) - POSITION('':'', INPUT_VALUE) - 1) AS INTEGER)
                               + CAST(SUBSTR(INPUT_VALUE,POSITION('':'', INPUT_VALUE, POSITION('':'', INPUT_VALUE)+1)+1) AS DECIMAL(10,6)),
           ''HOUR'',             3600 * INPUT_VALUE, 
           ''HOUR TO MINUTE'',   3600 * CAST(SUBSTR(INPUT_VALUE,1 , POSITION('':'', INPUT_VALUE)-1) AS INTEGER)
                               + 60 * CAST(SUBSTR(INPUT_VALUE,POSITION('':'', INPUT_VALUE)+1) AS INTEGER),
           ''HOUR TO SECOND'',   3600 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION('':'', INPUT_VALUE)-POSITION('' '', INPUT_VALUE)-1) AS INTEGER)
                               + 60 * CAST(SUBSTR(INPUT_VALUE, POSITION('':'', INPUT_VALUE)+1, POSITION('':'', INPUT_VALUE, POSITION('':'', INPUT_VALUE)+1) - POSITION('':'', INPUT_VALUE) - 1) AS INTEGER)
                               + CAST(SUBSTR(INPUT_VALUE,POSITION('':'', INPUT_VALUE, POSITION('':'', INPUT_VALUE)+1)+1) AS DECIMAL(10,6)),  
           ''MINUTE'',           60 * INPUT_VALUE,     
           ''MINUTE TO SECOND'', 60 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION('':'', INPUT_VALUE)-POSITION('' '', INPUT_VALUE)-1) AS INTEGER)
                               + CAST(SUBSTR(INPUT_VALUE, POSITION('':'', INPUT_VALUE)+1) AS DECIMAL(10,6)), 
           ''SECOND'',           INPUT_VALUE                                    
        )
END
';

CREATE OR REPLACE FUNCTION DEMO_DB.PUBLIC.PERIOD_LDIFF_UDF(PERIOD_1 VARCHAR(50), PERIOD_2 VARCHAR(50))
RETURNS VARCHAR
AS
' CASE WHEN PERIOD_OVERLAPS_UDF(PERIOD_2, PERIOD_1) = ''TRUE'' 
            AND PERIOD_BEGIN_UDF(PERIOD_1) < PERIOD_BEGIN_UDF(PERIOD_2) 
       THEN
        SUBSTR(PERIOD_1,1, POSITION(''*'',PERIOD_1)-1) || ''*'' || SUBSTR(PERIOD_2,1, POSITION(''*'',PERIOD_2)-1)
       ELSE
         NULL
       END '     
;

CREATE OR REPLACE FUNCTION DEMO_DB.PUBLIC.PERIOD_RDIFF_UDF(PERIOD_1 VARCHAR(50), PERIOD_2 VARCHAR(50))
RETURNS VARCHAR
AS
' CASE WHEN PERIOD_OVERLAPS_UDF(PERIOD_2, PERIOD_1) = ''TRUE'' 
            AND PERIOD_END_UDF(PERIOD_1) > PERIOD_END_UDF(PERIOD_2) 
       THEN
        SUBSTR(PERIOD_2,POSITION(''*'',PERIOD_2)+1) || ''*'' || SUBSTR(PERIOD_1,POSITION(''*'',PERIOD_1)+1)
       ELSE
         NULL
       END '     
;