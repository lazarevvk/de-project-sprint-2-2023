--------------------------------------------------------------------------------------------------------
--TASK1--

CREATE TABLE shipping_country_rates (
    id SERIAL PRIMARY KEY, 
    shipping_country VARCHAR(255) UNIQUE, 
    shipping_country_base_rate DECIMAL(10, 2)
);

INSERT INTO shipping_country_rates (shipping_country, shipping_country_base_rate)
SELECT distinct shipping_country, shipping_country_base_rate
FROM shipping;

select vendor_agreement_description from shipping
select * from shipping_country_rates 

--------------------------------------------------------------------------------------------------------
--TASK2--

drop table if exists shipping_agreement;
CREATE TABLE shipping_agreement (
    agreement_id SERIAL PRIMARY KEY, 
    agreement_number VARCHAR(255),
    agreement_rate DECIMAL(10, 2), 
    agreement_commission DECIMAL(5, 2) 
);


INSERT INTO shipping_agreement (agreement_id, agreement_number, agreement_rate, agreement_commission)
select distinct
    CAST(subarr[1] AS BIGINT) AS agreement_id,
    subarr[2] AS agreement_number,
    CAST(subarr[3] AS DECIMAL(10, 2)) AS agreement_rate,
    CAST(subarr[4] AS DECIMAL(5, 2)) AS agreement_commission
FROM (
    SELECT  regexp_split_to_array(vendor_agreement_description, E':') AS subarr
    FROM shipping
) AS subquery;

--------------------------------------------------------------------------------------------------------
--TASK3--

CREATE TABLE shipping_transfer (
    id SERIAL PRIMARY KEY, 
    transfer_type VARCHAR(255),
    transfer_model VARCHAR(255), 
    shipping_transfer_rate NUMERIC(14, 4) 
);

INSERT INTO shipping_transfer (transfer_type, transfer_model, shipping_transfer_rate)
SELECT DISTINCT
    CAST(subarr[1] AS VARCHAR(255)) AS transfer_type,
    CAST(subarr[2] AS VARCHAR(255)) AS transfer_model,
    shipping_transfer_rate
FROM (
    SELECT regexp_split_to_array(shipping_transfer_description, E':') AS subarr, shipping_transfer_rate
    FROM shipping
) AS subquery;

--------------------------------------------------------------------------------------------------------
--TASK4--

CREATE TABLE shipping_info (
    shipping_id SERIAL PRIMARY KEY,
    vendor_id INT, 
    payment_amount DECIMAL(14, 2),
    shipping_plan_datetime TIMESTAMP, 
    shipping_transfer_id BIGINT,
    shipping_agreement_id BIGINT,
    shipping_country_rate_id BIGINT
);

ALTER TABLE shipping_info
ADD CONSTRAINT fk_shipping_country_rate_id FOREIGN KEY (shipping_country_rate_id) REFERENCES shipping_country_rates(id);

ALTER TABLE shipping_info
ADD CONSTRAINT fk_shipping_agreement_id FOREIGN KEY (shipping_agreement_id) REFERENCES shipping_agreement(agreement_id);

ALTER TABLE shipping_info
ADD CONSTRAINT fk_shipping_transfer_id FOREIGN KEY (shipping_transfer_id) REFERENCES shipping_transfer(id);


INSERT INTO shipping_info (
    vendor_id, payment_amount, shipping_plan_datetime, shipping_transfer_id,  shipping_country_rate_id, shipping_agreement_id
)
select DISTINCT
	sp.vendorid, sp.payment_amount, sp.shipping_plan_datetime, st.id , scr.id, spa.agreement_id 
from 
	shipping sp
join 
	shipping_agreement spa on cast((regexp_split_to_array(sp.vendor_agreement_description, E':'))[1] as BIGINT) = spa.agreement_id
join
	shipping_country_rates scr on sp.shipping_country_base_rate = scr.shipping_country_base_rate 
join 
	shipping_transfer st on sp.shipping_transfer_rate = st.shipping_transfer_rate 


SELECT COUNT(*) AS count_of_duplicate_rows
FROM (
    SELECT COUNT(*) 
    FROM shipping_info
    GROUP BY vendor_id, payment_amount, shipping_plan_datetime, shipping_transfer_id,  shipping_country_rate_id, shipping_agreement_id
    HAVING COUNT(*) > 1
) AS subquery;

--------------------------------------------------------------------------------------------------------
--TASK5--

drop table shipping_status;
CREATE TABLE shipping_status (
    shipping_id BIGINT PRIMARY KEY, 
    status VARCHAR(255), 
    state VARCHAR(255), 
    shipping_start_fact_datetime TIMESTAMP, 
    shipping_end_fact_datetime TIMESTAMP 
);

WITH Recieved AS (
    SELECT 
        shippingid,
        MAX(state_datetime) as state_datetime
    FROM 
        shipping
	group by shippingid
)

INSERT INTO shipping_status (shipping_id, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
select recieved.shippingid, s2.state, s2.status, s.state_datetime as min, recieved.state_datetime as max from recieved
join shipping s on s.shippingid = recieved.shippingid
join shipping s2 on s2.state_datetime = recieved.state_datetime
where s.state = 'booked';

--------------------------------------------------------------------------------------------------------
--TASK6--

CREATE VIEW shipping_datamart AS
SELECT
    si.shipping_id,
    si.vendor_id,
    st.transfer_type,
    date_part('day', age(ss.shipping_end_fact_datetime, ss.shipping_start_fact_datetime)) AS full_day_at_shipping,
    CASE WHEN ss.shipping_end_fact_datetime > si.shipping_plan_datetime THEN 1 ELSE 0 END AS is_delay,
    CASE WHEN ss.status = 'finished' THEN 1 ELSE 0 END AS is_shipping_finish,
    CASE WHEN ss.shipping_end_fact_datetime > si.shipping_plan_datetime THEN 
        date_part('day', age(ss.shipping_end_fact_datetime, si.shipping_plan_datetime))
    ELSE 0 END AS delay_day_at_shipping,
    si.payment_amount,
    si.payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) AS vat,
    si.payment_amount * sa.agreement_commission AS profit
FROM
    shipping_info si
join
	shipping_status ss on si.shipping_id = ss.shipping_id 
JOIN
    shipping_transfer st ON si.shipping_transfer_id = st.id
JOIN
    shipping_country_rates scr ON si.shipping_country_rate_id = scr.id
JOIN
    shipping_agreement sa ON si.shipping_agreement_id = sa.agreement_id;

select * from shipping_datamart 



