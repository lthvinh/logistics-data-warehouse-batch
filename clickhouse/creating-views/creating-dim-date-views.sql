CREATE VIEW dim_created_date AS
SELECT
    date_key AS created_date_key,
    date AS created_date,
    year AS created_year,
    quarter AS created_quarter,
    month AS created_month,
    day AS created_day,
    day_of_week AS created_day_of_week,
    day_of_year AS created_day_of_year,
    week_of_year AS created_week_of_year,
    is_weekend AS created_is_weekend,
    week_name AS created_week_name,
    month_name AS created_month_name,
    quarter_name AS created_quarter_name
FROM dim_date;

CREATE VIEW dim_accepted_date AS
SELECT
    date_key AS accepted_date_key,
    date AS accepted_date,
    year AS accepted_year,
    quarter AS accepted_quarter,
    month AS accepted_month,
    day AS accepted_day,
    day_of_week AS accepted_day_of_week,
    day_of_year AS accepted_day_of_year,
    week_of_year AS accepted_week_of_year,
    is_weekend AS accepted_is_weekend,
    week_name AS accepted_week_name,
    month_name AS accepted_month_name,
    quarter_name AS accepted_quarter_name
FROM dim_date;

CREATE VIEW dim_in_transit_date AS
SELECT
    date_key AS in_transit_date_key,
    date AS in_transit_date,
    year AS in_transit_year,
    quarter AS in_transit_quarter,
    month AS in_transit_month,
    day AS in_transit_day,
    day_of_week AS in_transit_day_of_week,
    day_of_year AS in_transit_day_of_year,
    week_of_year AS in_transit_week_of_year,
    is_weekend AS in_transit_is_weekend,
    week_name AS in_transit_week_name,
    month_name AS in_transit_month_name,
    quarter_name AS in_transit_quarter_name
FROM dim_date;

CREATE VIEW dim_delivered_date AS
SELECT
    date_key AS delivered_date_key,
    date AS delivered_date,
    year AS delivered_year,
    quarter AS delivered_quarter,
    month AS delivered_month,
    day AS delivered_day,
    day_of_week AS delivered_day_of_week,
    day_of_year AS delivered_day_of_year,
    week_of_year AS delivered_week_of_year,
    is_weekend AS delivered_is_weekend,
    week_name AS delivered_week_name,
    month_name AS delivered_month_name,
    quarter_name AS delivered_quarter_name
FROM dim_date;

CREATE VIEW dim_delivery_date AS
SELECT
    date_key AS delivery_date_key,
    date AS delivery_date,
    year AS delivery_year,
    quarter AS delivery_quarter,
    month AS delivery_month,
    day AS delivery_day,
    day_of_week AS delivery_day_of_week,
    day_of_year AS delivery_day_of_year,
    week_of_year AS delivery_week_of_year,
    is_weekend AS delivery_is_weekend,
    week_name AS delivery_week_name,
    month_name AS delivery_month_name,
    quarter_name AS delivery_quarter_name
FROM dim_date;
