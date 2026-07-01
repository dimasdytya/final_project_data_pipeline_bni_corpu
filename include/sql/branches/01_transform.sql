TRUNCATE TABLE dim_branches;

INSERT INTO dim_branches (
    branch_id,
    branch_code,
    branch_name,
    city,
    province,
    region,
    branch_type,
    open_date,
    is_active,
    branch_age_years
)

SELECT
    branch_id,
    branch_code,
    branch_name,
    city,
    province,
    region,
    branch_type,
    TO_DATE(open_date,'YYYY-MM-DD'),
    LOWER(is_active) = 'true',
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, TO_DATE(open_date,'YYYY-MM-DD')))::INT
FROM stg_branches;