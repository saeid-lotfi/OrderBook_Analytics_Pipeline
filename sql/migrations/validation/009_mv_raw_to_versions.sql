CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.mv_raw_to_versions
TO {database}.orders_versions_local
AS
SELECT
    JSONExtractUInt(raw_message, 'id') AS id,
    JSONExtractUInt(raw_message, '__LSN') AS lsn,

    JSONExtractString(raw_message, '__op') AS op,

    fromUnixTimestamp64Milli(
        toInt64(JSONExtractUInt(raw_message, '__EVENT_TS')),
        'Asia/Tehran'
    ) AS event_ts,

    fromUnixTimestamp64Milli(
        toInt64(JSONExtractUInt(raw_message, '__KAFKA_EVENT_TS')),
        'Asia/Tehran'
    ) AS kafka_event_ts,

    parseDateTime64BestEffortOrNull(
        JSONExtractString(raw_message, 'created_at'),
        3,
        'Asia/Tehran'
    ) AS created_at,

    parseDateTime64BestEffortOrNull(
        JSONExtractString(raw_message, 'updated_at'),
        3,
        'Asia/Tehran'
    ) AS updated_at,

    JSONExtractUInt(raw_message, 'market_id') AS market_id,
    JSONExtractUInt(raw_message, 'user_id') AS user_id,

    JSONExtractString(raw_message, 'type') AS side,
    JSONExtractString(raw_message, 'mode') AS mode,
    JSONExtractString(raw_message, 'state') AS state,

    toDecimal128OrNull(
        JSONExtractString(raw_message, 'price'),
        7
    ) AS price,

    toDecimal256OrNull(
        JSONExtractString(raw_message, 'amount1'),
        18
    ) AS amount1,

    toDecimal256OrNull(
        JSONExtractString(raw_message, 'amount2'),
        18
    ) AS amount2,

    toDecimal256OrNull(
        JSONExtractString(raw_message, 'exchanged1'),
        18
    ) AS exchanged1,

    toDecimal256OrNull(
        JSONExtractString(raw_message, 'exchanged2'),
        18
    ) AS exchanged2,

    JSONExtractString(raw_message, 'service') AS service,
    JSONExtractBool(raw_message, 'bot') AS bot,
    JSONExtractString(raw_message, 'bot_type') AS bot_type,

    JSONExtractBool(raw_message, 'req_to_cancel') AS req_to_cancel,
    JSONExtractBool(raw_message, 'settled_in_match') AS settled_in_match,

    kafka_topic,
    kafka_partition,
    kafka_offset,
    ingested_at

FROM {database}.raw_events_local

WHERE
    isValidJSON(raw_message)
    AND JSONExtractUInt(raw_message, 'id') > 0
    AND JSONExtractUInt(raw_message, '__LSN') > 0
    AND JSONExtractString(raw_message, '__op') IN ('c', 'u', 'd', 'r')
    AND JSONExtractUInt(raw_message, '__EVENT_TS') > 0
    AND JSONExtractUInt(raw_message, '__KAFKA_EVENT_TS') > 0
    AND JSONExtractString(raw_message, 'type') IN ('buy', 'sell')
    AND JSONExtractString(raw_message, 'mode') != ''
    AND JSONExtractString(raw_message, 'state') != ''
    AND JSONExtractUInt(raw_message, 'market_id') > 0
    AND toDecimal128OrNull(
        JSONExtractString(raw_message, 'price'), 7
    ) IS NOT NULL
    AND toDecimal256OrNull(
        JSONExtractString(raw_message, 'amount1'), 18
    ) IS NOT NULL
    AND toDecimal256OrNull(
        JSONExtractString(raw_message, 'amount2'), 18
    ) IS NOT NULL
    AND toDecimal256OrNull(
        JSONExtractString(raw_message, 'exchanged1'), 18
    ) IS NOT NULL
    AND toDecimal256OrNull(
        JSONExtractString(raw_message, 'exchanged2'), 18
    ) IS NOT NULL;