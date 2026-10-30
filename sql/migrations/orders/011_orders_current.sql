CREATE VIEW IF NOT EXISTS {database}.orders_current
AS
SELECT
    id,
    tupleElement(latest, 1) AS lsn,
    tupleElement(latest, 2) AS op,
    tupleElement(latest, 3) AS market_id,
    tupleElement(latest, 4) AS user_id,
    tupleElement(latest, 5) AS side,
    tupleElement(latest, 6) AS mode,
    tupleElement(latest, 7) AS state,
    tupleElement(latest, 8) AS price,
    tupleElement(latest, 9) AS amount1,
    tupleElement(latest, 10) AS amount2,
    tupleElement(latest, 11) AS exchanged1,
    tupleElement(latest, 12) AS exchanged2,
    tupleElement(latest, 13) AS service,
    tupleElement(latest, 14) AS bot,
    tupleElement(latest, 15) AS req_to_cancel,
    tupleElement(latest, 16) AS settled_in_match,
    tupleElement(latest, 17) AS event_ts
FROM
(
    SELECT
        id,
        argMax(
            tuple(
                lsn,
                op,
                market_id,
                user_id,
                side,
                mode,
                state,
                price,
                amount1,
                amount2,
                exchanged1,
                exchanged2,
                service,
                bot,
                req_to_cancel,
                settled_in_match,
                event_ts
            ),
            tuple(lsn, kafka_partition, kafka_offset)
        ) AS latest
    FROM {database}.orders_latest
    GROUP BY id
);
