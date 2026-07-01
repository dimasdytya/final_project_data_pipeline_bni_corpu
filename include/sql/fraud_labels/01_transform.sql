UPDATE fact_transactions ft

SET

    is_fraud = sf.is_fraud,

    fraud_type = sf.fraud_type,

    fraud_score = sf.fraud_score,

    flagged_at = TO_TIMESTAMP(
        sf.flagged_at,
        'YYYY-MM-DD HH24:MI:SS'
    )

FROM stg_fraud_labels sf

WHERE ft.transaction_id = sf.transaction_id;