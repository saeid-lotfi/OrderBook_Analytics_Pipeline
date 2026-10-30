"""Independent Decimal arithmetic checks on SQL-generated persisted ladders."""
import sys
from decimal import Decimal, localcontext
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.orderbook.clickhouse.client import ClickHouseManager, load_clickhouse_config


def main():
    manager = ClickHouseManager(load_clickhouse_config())
    client = manager.get_client(manager.get_primary_host())
    try:
        rows = client.query(f'SELECT * FROM {manager.database}.current_book').named_results()
        checked = 0
        with localcontext() as ctx:
            ctx.prec = 100
            for row in rows:
                for side, levels in [('bid', row['bids']), ('ask', row['asks'])]:
                    prices = [x[0] for x in levels]
                    assert prices == sorted(set(prices), reverse=(side == 'bid'))
                    assert row[f'n_{side}_levels'] == len(levels)
                    assert row[f'best_{side}'] == (prices[0] if prices else None)
                    assert row[f'second_{side}'] == (prices[1] if len(prices) > 1 else None)
                    for i, k in enumerate(map(Decimal, ['.001', '.005', '.01', '.02', '.05'])):
                        if side == 'bid':
                            eligible = [x for x in levels if x[0] >= prices[0] * (1-k)]
                        else:
                            eligible = [x for x in levels if x[0] <= prices[0] * (1+k)]
                        assert row[f'{side}_depth_count'][i] == len(eligible)
                        assert row[f'{side}_depth_volume'][i] == sum((x[2] for x in eligible), Decimal(0))
                    for price, base, notional in levels:
                        assert notional == price * base, (price, base, notional)
                    gap = abs(prices[0]-prices[1]) if len(prices) > 1 else None
                    assert row[f'{side}_gap'] == gap
                    thresholds = ([50000000, 100000000, 200000000, 500000000, 1000000000,
                                   2000000000, 5000000000, 10000000000]
                                  if row['quote_currency'] == 'IRT' else
                                  [500, 2000, 5000, 10000, 20000, 50000, 100000])
                    assert row['fill_thresholds'] == thresholds
                    for threshold, impact in zip(row['fill_thresholds'], row[f'{side}_fill_impact']):
                        cumulative = Decimal(0)
                        expected = None
                        for price, base, notional in levels:
                            cumulative += notional
                            if cumulative >= threshold:
                                expected = abs(price-prices[0]) / prices[0]
                                break
                        assert (expected is None) == (impact is None)
                        if expected is not None:
                            assert abs(expected-impact) < Decimal('1e-18'), (side, expected, impact)
                bid, ask = row['best_bid'], row['best_ask']
                assert row['spread'] == (ask-bid if bid is not None and ask is not None else None)
                assert row['is_crossed'] == int(bid is not None and ask is not None and bid >= ask)
                checked += 1
        assert checked > 0, 'No fresh snapshots'
        print(f'PASS: Decimal metric arithmetic, ordering, counts and nulls for {checked} markets')
    finally:
        client.close()


if __name__ == '__main__':
    main()
