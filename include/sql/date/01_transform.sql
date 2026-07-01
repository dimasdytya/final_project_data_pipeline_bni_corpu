TRUNCATE TABLE dim_date;

INSERT INTO dim_date (

    date_id,
    full_date,

    year,
    quarter,
    month,
    month_name,

    week_of_year,

    day_of_month,
    day_of_week,
    day_name,

    is_weekend,
    is_holiday

)

SELECT

    date_id,

    TO_DATE(full_date,'YYYY-MM-DD'),

    year,
    quarter,
    month,
    month_name,

    week_of_year,

    day_of_month,
    day_of_week,
    day_name,

    is_weekend,
    is_holiday

FROM stg_date;