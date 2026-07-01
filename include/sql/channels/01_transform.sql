TRUNCATE TABLE dim_channels;

INSERT INTO dim_channels (
    channel_id,
    channel_code,
    channel_name,
    channel_category,
    is_digital,
    description
)

SELECT
    channel_id,
    channel_code,
    channel_name,
    channel_category,
    LOWER(is_digital) = 'true',
    description

FROM stg_channels;