CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.mv_raw_to_dead_letter
TO {database}.dead_letter_events_local
AS
SELECT
    raw_message,

    multiIf(
        NOT isValidJSON(raw_message),
            'invalid_json',

        JSONExtractUInt(raw_message, 'id') = 0,
            'invalid_order_id',

        JSONExtractUInt(raw_message, '__LSN') = 0,
            'invalid_lsn',

        JSONExtractString(raw_message, '__op')
            NOT IN ('c', 'u', 'd', 'r'),
            'invalid_operation',

        JSONExtractUInt(raw_message, '__EVENT_TS') = 0
            OR JSONExtractUInt(raw_message, '__KAFKA_EVENT_TS') = 0,
            'invalid_event_timestamp',

        JSONExtractString(raw_message, 'type')
            NOT IN ('buy', 'sell'),
            'invalid_side',

        JSONExtractString(raw_message, 'mode') = ''
            OR JSONExtractString(raw_message, 'state') = '',
            'missing_order_status',

        JSONExtractUInt(raw_message, 'market_id') = 0,
            'invalid_market_id',

        toDecimal128OrNull(
            JSONExtractString(raw_message, 'price'), 7
        ) IS NULL,
            'invalid_price',

        toDecimal256OrNull(
            JSONExtractString(raw_message, 'amount1'), 18
        ) IS NULL
        OR toDecimal256OrNull(
            JSONExtractString(raw_message, 'amount2'), 18
        ) IS NULL
        OR toDecimal256OrNull(
            JSONExtractString(raw_message, 'exchanged1'), 18
        ) IS NULL
        OR toDecimal256OrNull(
            JSONExtractString(raw_message, 'exchanged2'), 18
        ) IS NULL,
            'invalid_amount',

        'unknown_validation_error'
    ) AS error_reason,

    kafka_topic,
    kafka_partition,
    kafka_offset,
    ingested_at

FROM {database}.raw_events_local

WHERE
    NOT isValidJSON(raw_message)
    OR JSONExtractUInt(raw_message, 'id') = 0
    OR JSONExtractUInt(raw_message, '__LSN') = 0
    OR JSONExtractString(raw_message, '__op')
        NOT IN ('c', 'u', 'd', 'r')
    OR JSONExtractUInt(raw_message, '__EVENT_TS') = 0
    OR JSONExtractUInt(raw_message, '__KAFKA_EVENT_TS') = 0
    OR JSONExtractString(raw_message, 'type')
        NOT IN ('buy', 'sell')
    OR JSONExtractString(raw_message, 'mode') = ''
    OR JSONExtractString(raw_message, 'state') = ''
    OR JSONExtractUInt(raw_message, 'market_id') = 0
    OR toDecimal128OrNull(
        JSONExtractString(raw_message, 'price'), 7
    ) IS NULL
    OR toDecimal256OrNull(
        JSONExtractString(raw_message, 'amount1'), 18
    ) IS NULL
    OR toDecimal256OrNull(
        JSONExtractString(raw_message, 'amount2'), 18
    ) IS NULL
    OR toDecimal256OrNull(
        JSONExtractString(raw_message, 'exchanged1'), 18
    ) IS NULL
    OR toDecimal256OrNull(
        JSONExtractString(raw_message, 'exchanged2'), 18
    ) IS NULL;