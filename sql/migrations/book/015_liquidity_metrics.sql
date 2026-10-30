CREATE OR REPLACE VIEW {database}.liquidity_metrics
AS
WITH
    [
        toDecimal256('0.001', 7),
        toDecimal256('0.005', 7),
        toDecimal256('0.01', 7),
        toDecimal256('0.02', 7),
        toDecimal256('0.05', 7)
    ] AS bands

SELECT
    market_id,

    quote_currency,
    bids,
    asks,

    best_bid,
    best_ask,
    second_bid,
    second_ask,

    if(
        isNotNull(best_bid) AND isNotNull(best_ask),
        best_ask - best_bid,
        NULL
    ) AS spread,

    if(
        isNotNull(best_bid) AND isNotNull(second_bid),
        best_bid - second_bid,
        NULL
    ) AS bid_gap,

    if(
        isNotNull(best_ask) AND isNotNull(second_ask),
        second_ask - best_ask,
        NULL
    ) AS ask_gap,

    length(bids) AS n_bid_levels,
    length(asks) AS n_ask_levels,

    if(
        isNotNull(best_bid) AND isNotNull(best_ask),
        toUInt8(best_bid >= best_ask),
        toUInt8(0)
    ) AS is_crossed,

    arrayMap(
        k -> arrayCount(
            level ->
                level.1 >= best_bid * (1 - k),
            bids
        ),
        bands
    ) AS bid_depth_count,

    arrayMap(
        k -> arrayCount(
            level ->
                level.1 <= best_ask * (1 + k),
            asks
        ),
        bands
    ) AS ask_depth_count,

    arrayMap(
        k -> arrayReduce(
            'sum',
            arrayMap(
                level -> level.3,
                arrayFilter(
                    level ->
                        level.1 >= best_bid * (1 - k),
                    bids
                )
            )
        ),
        bands
    ) AS bid_depth_volume,

    arrayMap(
        k -> arrayReduce(
            'sum',
            arrayMap(
                level -> level.3,
                arrayFilter(
                    level ->
                        level.1 <= best_ask * (1 + k),
                    asks
                )
            )
        ),
        bands
    ) AS ask_depth_volume,

    thresholds AS fill_thresholds,

    arrayMap(
        threshold ->
            if(
                isNull(best_bid)
                OR best_bid = 0
                OR arrayFirstIndex(
                    cumulative -> cumulative >= threshold,
                    bid_cumulative
                ) = 0,
                NULL,
                divideDecimal(
                    best_bid
                    -
                    tupleElement(
                        arrayElementOrNull(
                            bids,
                            arrayFirstIndex(
                                cumulative -> cumulative >= threshold,
                                bid_cumulative
                            )
                        ),
                        1
                    )
                    , best_bid, 18
                )
            ),
        thresholds
    ) AS bid_fill_impact,

    arrayMap(
        threshold ->
            if(
                isNull(best_ask)
                OR best_ask = 0
                OR arrayFirstIndex(
                    cumulative -> cumulative >= threshold,
                    ask_cumulative
                ) = 0,
                NULL,
                divideDecimal(
                    tupleElement(
                        arrayElementOrNull(
                            asks,
                            arrayFirstIndex(
                                cumulative -> cumulative >= threshold,
                                ask_cumulative
                            )
                        ),
                        1
                    )
                    -
                    best_ask
                    , best_ask, 18
                )
            ),
        thresholds
    ) AS ask_fill_impact

FROM
(
    SELECT
        markets.market_id AS market_id,
        ladders.bids AS bids,
        ladders.asks AS asks,

        if(
            length(bids) > 0,
            toNullable(bids[1].1),
            NULL
        ) AS best_bid,

        if(
            length(asks) > 0,
            toNullable(asks[1].1),
            NULL
        ) AS best_ask,

        if(
            length(bids) > 1,
            toNullable(bids[2].1),
            NULL
        ) AS second_bid,

        if(
            length(asks) > 1,
            toNullable(asks[2].1),
            NULL
        ) AS second_ask,

        markets.quote_ccy AS quote_currency,

        if(
            markets.quote_ccy = 'IRT',
            [
                toDecimal256('50000000', 0),
                toDecimal256('100000000', 0),
                toDecimal256('200000000', 0),
                toDecimal256('500000000', 0),
                toDecimal256('1000000000', 0),
                toDecimal256('2000000000', 0),
                toDecimal256('5000000000', 0),
                toDecimal256('10000000000', 0)
            ],
            [
                toDecimal256('500', 0),
                toDecimal256('2000', 0),
                toDecimal256('5000', 0),
                toDecimal256('10000', 0),
                toDecimal256('20000', 0),
                toDecimal256('50000', 0),
                toDecimal256('100000', 0)
            ]
        ) AS thresholds,

        arrayCumSum(
            arrayMap(level -> level.3, bids)
        ) AS bid_cumulative,

        arrayCumSum(
            arrayMap(level -> level.3, asks)
        ) AS ask_cumulative

    FROM {database}.markets AS markets

    LEFT JOIN {database}.book_ladders AS ladders
        ON ladders.market_id = markets.market_id
);
