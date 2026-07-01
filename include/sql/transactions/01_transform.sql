TRUNCATE TABLE fact_transactions;

INSERT INTO fact_transactions (

    transaction_id,
    date_id,
    account_id,
    customer_id,
    branch_id,
    channel_id,
    transaction_code,
    transaction_at,
    transaction_type,
    amount,
    balance_before,
    balance_after,
    status,
    reference_no,
    is_fraud,
    fraud_type,
    fraud_score,
    flagged_at

)

SELECT

    s.transaction_id,

    d.date_id,

    s.account_id,
    s.customer_id,
    s.branch_id,
    s.channel_id,

    s.transaction_code,

    TO_TIMESTAMP(
        s.transaction_at,
        'YYYY-MM-DD HH24:MI:SS'
    ),

    s.transaction_type,

    s.amount,
    s.balance_before,
    s.balance_after,

    s.status,
    s.reference_no,

    FALSE,
    NULL,
    NULL,
    NULL

FROM stg_transactions s

JOIN dim_date d
ON d.full_date = TO_DATE(
    s.transaction_date,
    'YYYY-MM-DD'
);