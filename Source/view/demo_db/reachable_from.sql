/* <sc-view> DEMO_DB.REACHABLE_FROM </sc-view> */
-- Recursive view  
REPLACE RECURSIVE VIEW DEMO_DB.REACHABLE_FROM (DESTINATION, COST, LEGS) AS (
      SELECT 
        ROOT.DESTINATION, 
        ROOT.COST, 
        1 AS LEGS
      FROM 
        DEMO_DB.FLIGHTS AS ROOT
      WHERE 
        ROOT.SOURCE = 'Paris'
    UNION ALL
      SELECT 
        OUTT.DESTINATION, 
        INN.COST + OUTT.COST, 
        INN.LEGS + 1 AS LEGS
      FROM 
        REACHABLE_FROM AS INN, 
        DEMO_DB.FLIGHTS AS OUTT
      WHERE 
        INN.DESTINATION = OUTT.SOURCE
        AND INN.LEGS <= 20);        


