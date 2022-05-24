CREATE OR REPLACE PROCEDURE public.pratish_customer_rating_daily_metadata_proc()
  LANGUAGE plpgsql
AS $$
DECLARE

BEGIN

drop table if exists pratish_reports_customer_rating_daily_metadata;
create table pratish_reports_customer_rating_daily_metadata as
select customer_phone,
    max(date(assigned_date_time)) as last_assigned_date,
    count(*) last_30_days_shipment,
    count(distinct company_id) last_30_days_company_count,
    sum(shipment_total) last_30_days_value,
    sum(case when is_return=1 then 1 else 0 end) last_30_days_is_return_shipment,
    sum(case when cod=0  then 1 else 0 end) last_30_days_prepaid_orders
    from reports_awb_wise_report where  shipment_status_id<>8 and date(assigned_date_time) >= dateadd(days, -30, current_date) group by 1;

commit;
 RAISE INFO 'initial table created!';

-- 6. LTV: shipments and revenue
ALTER TABLE pratish_reports_customer_rating_daily_metadata ADD shipments_ltv int;
ALTER TABLE pratish_reports_customer_rating_daily_metadata ADD revenue_ltv int;
ALTER TABLE pratish_reports_customer_rating_daily_metadata ADD first_shipment_date date;
UPDATE pratish_reports_customer_rating_daily_metadata
SET
  shipments_ltv = l.shipments_ltv,
  revenue_ltv = l.revenue_ltv,
  first_shipment_date = l.first_shipment_date
FROM (
select
  customer_phone,
  min(date(assigned_date_time)) as first_shipment_date,
  count(*) as shipments_ltv,
  sum(shipment_total) as revenue_ltv
from reports_awb_wise_report
where shipment_status_id <> 8
  and date(assigned_date_time) <= current_date
  and customer_phone in (select customer_phone from pratish_reports_customer_rating_daily_metadata )
group by 1) l
WHERE pratish_reports_customer_rating_daily_metadata.customer_phone=l.customer_phone ;


commit;
 RAISE INFO 'lifetime shipment added!';


ALTER TABLE pratish_reports_customer_rating_daily_metadata ADD last_30_days_recency decimal(8,2);
UPDATE pratish_reports_customer_rating_daily_metadata
SET
  last_30_days_recency = r3.last_30_days_recency
FROM (
  select
    customer_phone,
    AVG(DATEDIFF(days, assigned_date, current_date)::decimal(10,3)) as last_30_days_recency
  from (
    select customer_phone, assigned_date, row_number() over (partition by customer_phone order by assigned_date desc ) as rn
    from (
      select
       distinct date(assigned_date_time) assigned_date,
        customer_phone
      from reports_awb_wise_report
      where shipment_status_id <>8
        and assigned_date >= dateadd(days, -30, current_date)
        and customer_phone in (select customer_phone from pratish_reports_customer_rating_daily_metadata )
      group by 1, 2
      order by customer_phone, assigned_date desc))
  where rn <=3 
  group by 1) r3
WHERE pratish_reports_customer_rating_daily_metadata.customer_phone=r3.customer_phone ;


commit;
 RAISE INFO 'last 30 days recency added!';


ALTER TABLE pratish_reports_customer_rating_daily_metadata ADD last_5months_shipment int;
ALTER TABLE pratish_reports_customer_rating_daily_metadata ADD last_5months_last_assigned_date date;
ALTER TABLE pratish_reports_customer_rating_daily_metadata ADD last_5months_company_count int;
ALTER TABLE pratish_reports_customer_rating_daily_metadata ADD last_5months_value int;
ALTER TABLE pratish_reports_customer_rating_daily_metadata ADD last_5months_is_return_shipment int;
ALTER TABLE pratish_reports_customer_rating_daily_metadata ADD last_5months_prepaid_orders int;
UPDATE pratish_reports_customer_rating_daily_metadata
SET
  last_5months_shipment = l.last_5months_shipment,
  last_5months_last_assigned_date = l.last_5months_last_assigned_date,
  last_5months_company_count = l.last_5months_company_count,
  last_5months_value = l.last_5months_value,
  last_5months_is_return_shipment = l.last_5months_is_return_shipment,
  last_5months_prepaid_orders = l.last_5months_prepaid_orders
FROM (
select customer_phone,
    count(*) last_5months_shipment,
    max(date(assigned_date_time)) as last_5months_last_assigned_date,
    count(distinct company_id) last_5months_company_count,
    sum(shipment_total) last_5months_value,
    sum(case when is_return=1 then 1 else 0 end) last_5months_is_return_shipment,
    sum(case when cod=0  then 1 else 0 end) last_5months_prepaid_orders
    from reports_awb_wise_report where  shipment_status_id<>8 and date(assigned_date_time) between dateadd(mm, -6, current_date) and dateadd(days, -30, current_date)
    and customer_phone in (select customer_phone from pratish_reports_customer_rating_daily_metadata ) group by 1 ) l 
WHERE pratish_reports_customer_rating_daily_metadata.customer_phone=l.customer_phone ;


commit;
 RAISE INFO 'past 5 months shipment added!';


-- 6. LTV: shipments and revenue
ALTER TABLE pratish_reports_customer_rating_daily_metadata ADD last_5months_shipments_ltv int;
ALTER TABLE pratish_reports_customer_rating_daily_metadata ADD last_5months_revenue_ltv int;
UPDATE pratish_reports_customer_rating_daily_metadata
SET
  last_5months_shipments_ltv = l.last_5months_shipments_ltv,
  last_5months_revenue_ltv = l.last_5months_revenue_ltv
FROM (
select
  customer_phone,
  count(*) as last_5months_shipments_ltv,
  sum(shipment_total) as last_5months_revenue_ltv
from reports_awb_wise_report
where shipment_status_id <> 8
  and date(assigned_date_time) <= dateadd(days, -30, current_date)
  and customer_phone in (select customer_phone from pratish_reports_customer_rating_daily_metadata )
group by 1) l
WHERE pratish_reports_customer_rating_daily_metadata.customer_phone=l.customer_phone ;


commit;
 RAISE INFO 'past 5 months lifetime shipment added!';

ALTER TABLE pratish_reports_customer_rating_daily_metadata ADD last_5months_recency decimal(8,2);
UPDATE pratish_reports_customer_rating_daily_metadata
SET
  last_5months_recency = r3.last_5months_recency
FROM (
  select
    customer_phone,
    AVG(DATEDIFF(days, assigned_date, dateadd(days, -30, current_date))::decimal(10,3)) as last_5months_recency
  from (
    select customer_phone, assigned_date, row_number() over (partition by customer_phone order by assigned_date desc ) as rn
    from (
      select
       distinct date(assigned_date_time) assigned_date,
        customer_phone
      from reports_awb_wise_report
      where shipment_status_id <>8
        and assigned_date between dateadd(mm, -6, current_date) and dateadd(days, -30, current_date)
        and customer_phone in (select customer_phone from pratish_reports_customer_rating_daily_metadata )
      group by 1, 2
      order by customer_phone, assigned_date desc))
  where rn <=3 
  group by 1) r3
WHERE pratish_reports_customer_rating_daily_metadata.customer_phone=r3.customer_phone ;

ALTER TABLE pratish_reports_customer_rating_daily_metadata ADD updated_at datetime;
UPDATE pratish_reports_customer_rating_daily_metadata set updated_at = getdate() + interval '1h' * 5.5;



commit;
 RAISE INFO 'past 5 months recency added!';


END;
$$;

