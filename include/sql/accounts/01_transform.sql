TRUNCATE TABLE dim_accounts;

INSERT INTO dim_accounts (
    account_id,
    account_no,
    account_type,
    product_name,
    currency,
    open_date,
    close_date,
    status,
    interest_rate,
    customer_id,
    branch_id,
    account_age_years,
    is_closed
)

SELECT
    account_id,
    account_no,
    account_type,
    product_name,
    currency,

    TO_DATE(open_date,'YYYY-MM-DD'),

    CASE
        WHEN close_date IS NULL OR close_date = ''
        THEN NULL
        ELSE TO_DATE(close_date,'YYYY-MM-DD')
    END,

    status,

    interest_rate,

    customer_id,

    branch_id,

    EXTRACT(
        YEAR FROM AGE(
            CURRENT_DATE,
            TO_DATE(open_date,'YYYY-MM-DD')
        )
    )::INT,

    status = 'CLOSED'

FROM stg_accounts;