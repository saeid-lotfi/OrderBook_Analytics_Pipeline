"""Read-only scoring tool. Ground truth is used here only, never by the pipeline."""
import argparse
import json
import math
import sys
from decimal import Decimal
from pathlib import Path

# Permit both `python scripts/validate.py` and module invocation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.orderbook.clickhouse.client import ClickHouseManager, load_clickhouse_config


def oracle_equal(actual, expected):
    if actual is None or expected is None:
        return actual is expected
    # The supplied oracle serializes binary64 numbers; permit one oracle ULP.
    tolerance = Decimal(str(math.ulp(float(expected))))
    return abs(Decimal(actual) - expected) <= tolerance


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--truth', type=Path, default=Path('task/data/ground_truth.json'))
    parser.add_argument('--config', type=Path, default=Path('config/clickhouse.json'))
    args = parser.parse_args()
    truth = json.loads(args.truth.read_text(), parse_float=Decimal, parse_int=Decimal)
    manager = ClickHouseManager(load_clickhouse_config(args.config))
    rows = manager.query(manager.get_primary_host(), f'''
        SELECT market_id, best_bid, best_ask, second_bid, second_ask,
               spread, n_bid_levels, n_ask_levels, is_crossed
        FROM {manager.database}.current_book ORDER BY market_id''')
    fields = ('best_bid', 'best_ask', 'second_bid', 'second_ask', 'spread',
              'n_bid_levels', 'n_ask_levels', 'is_crossed')
    actual = {str(row[0]): dict(zip(fields, row[1:])) for row in rows}
    exact = passed = full = 0
    for market_id, expected in truth['markets'].items():
        result = actual.get(market_id)
        if result is None:
            print(f'FAIL {market_id:>2} {expected["market"]}: missing or stale snapshot')
            continue
        top = all(oracle_equal(result[k], expected[k]) for k in ('best_bid', 'best_ask'))
        strict = all(result[k] == expected[k] for k in ('best_bid', 'best_ask'))
        differences = [f'{k}: actual={result[k]} oracle={expected[k]}' for k in fields
                       if not oracle_equal(result[k], expected[k])]
        exact += strict
        passed += top
        full += not differences
        print(f'{"PASS" if top else "FAIL"} {market_id:>2} {expected["market"]}'
              f' exact_top={strict}' + ('; ' + '; '.join(differences) if differences else ''))
    reference = {str(row[0]) for row in manager.query(manager.get_primary_host(),
                 f'SELECT market_id FROM {manager.database}.markets')}
    unexpected = set(actual) - reference
    missing = reference - set(actual)
    unscored = set(actual) - set(truth['markets'])
    two_sided_unscored = [k for k in unscored if actual[k]['n_bid_levels']
                          and actual[k]['n_ask_levels']]
    total = len(truth['markets'])
    print(f'Top of book: {passed}/{total} within one oracle ULP; '
          f'{exact}/{total} exact decimal matches. All scalar fields: {full}/{total}.')
    print(f'Oracle omits {len(unscored)} reference markets: {sorted(unscored)}; '
          f'unexpected_two_sided={two_sided_unscored}.')
    if unexpected or missing:
        print(f'Unknown markets: {sorted(unexpected)}; missing reference markets: {sorted(missing)}')
    print('Money in the pipeline is Decimal; ULP tolerance applies only to the supplied oracle.')
    return 0 if passed == total and not unexpected and not missing and not two_sided_unscored else 1


if __name__ == '__main__':
    raise SystemExit(main())
