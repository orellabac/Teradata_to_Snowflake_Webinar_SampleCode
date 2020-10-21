/* <sc-view> DEMO_DB.vtruncround</sc-view> */
-- Trunc and round
-- With a missing dependency.
REPLACE VIEW DEMO_DB.vtruncround
AS
select 
    region, 
    TRUNC(DATEFIRSTPURCHASE, 'MON') TRUNC_DATE,
    TRUNC(DATEFIRSTPURCHASE) TRUNC_DATE_DD,
    ROUND(DATEFIRSTPURCHASE, 'RM') RND_DATE,
    avg(yearlyincome) avg_annual_income,
    TRUNC(AVG(YEARLYINCOME)) TRUNC_INCOME,
    TRUNC(AVG(YEARLYINCOME), 2) TRNC_INCOME_2,
    ROUND(AVG(TOTALCHILDREN),0) RND_AVG_CHILDREN,
    avg(totalchildren) avg_children, 
    ROUND(AVG(AGE)) RND_AVG_AGE,
    avg(age) avg_age 
from DEMO_DB.vtargetmail
where datefirstpurchase in ('2011-02-08', '2011-02-09')
group by 
    region, 
    TRUNC(DATEFIRSTPURCHASE, 'MON'),
    TRUNC(DATEFIRSTPURCHASE),
    ROUND(DATEFIRSTPURCHASE, 'RM');


