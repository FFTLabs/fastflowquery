# Official TPC-H dbgen SF1 Fixture Output

This directory stores generated `.tbl` files from official-style TPC-H `dbgen` tooling.

Generate SF1:

```bash
make tpch-dbgen-sf1
```

Expected generated files:

1. `customer.tbl`
2. `lineitem.tbl`
3. `nation.tbl`
4. `orders.tbl`
5. `part.tbl`
6. `partsupp.tbl`
7. `region.tbl`
8. `supplier.tbl`
9. `manifest.json`

Notes:

1. `.tbl` files are intentionally gitignored.
2. `manifest.json` includes file sizes, row counts, hashes, and pinned source ref metadata.
