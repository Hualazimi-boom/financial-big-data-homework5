"""Independent set-based oracle; checks all rows, ordering, and totals."""
from pathlib import Path
from collections import Counter
import sys

root = Path(__file__).resolve().parents[1]
paths = [Path(p) for p in sys.argv[1:]] if len(sys.argv) == 5 else [
    root / 'input/A.txt', root / 'input/B.txt',
    root / 'output_task1/part-r-00000', root / 'output_task2/part-r-00000']
if len(paths) != 4:
    raise SystemExit('Usage: verify.py [A B task1-part task2-part]')

def read_set(path):
    rows = [line.split() for line in path.read_text(encoding='utf-8-sig').splitlines() if line.strip()]
    assert all(len(row) == 2 for row in rows)
    return {(int(i), s) for i, s in rows}, len(rows)

a, a_rows = read_set(paths[0])
b, b_rows = read_set(paths[1])
expected = [(i, s, 'BOTH' if (i, s) in a & b else 'A_ONLY' if (i, s) in a else 'B_ONLY')
            for i, s in sorted(a | b)]
actual = [(int(i), s, relation) for i, s, relation in
          (line.split() for line in paths[2].read_text().splitlines())]
assert actual == expected, 'Task1 mismatch (including ordering or duplicate rows)'
expected_counts = []
for i in sorted({i for i, s in a | b}):
    c = Counter(relation for index, stock, relation in expected if index == i)
    expected_counts.append((i, c['BOTH'], c['A_ONLY'], c['B_ONLY'], sum(c.values())))
actual_counts = [tuple(map(int, line.split())) for line in paths[3].read_text().splitlines()]
assert actual_counts == expected_counts, 'Task2 mismatch'
print(f'PASS: A rows={a_rows}, unique={len(a)}; B rows={b_rows}, unique={len(b)}')
print(f'PASS: task1={len(expected)} rows, fully verified and sorted; task2={len(expected_counts)} rows')
print('index\tBOTH\tA_ONLY\tB_ONLY\tUNION')
for row in expected_counts:
    print('\t'.join(map(str, row)))
