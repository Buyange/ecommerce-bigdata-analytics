# Distributed Multi-Model Analytics for E-commerce Data

AUCA Big Data Analytics Final Project (individual): MongoDB + HBase + Apache Spark.

## Architecture

```
dataset_generator.py --> JSON files (users, products, categories, transactions, sessions)
        |
        +--> MongoDB (documents): catalogue, users, orders, sessions   --> aggregation pipelines
        +--> HBase (wide column): session time series, product metrics --> row-key range scans
        +--> Spark: cleans the JSON to Parquet, cohort analysis, Spark SQL,
                    and an integrated funnel job that reads BOTH databases
        +--> matplotlib: four charts in viz/figures
```

| Folder | Contents |
|---|---|
| `scripts/` | Configurable data generator and a data exploration script |
| `mongo/` | Loader, aggregation pipelines, connection test |
| `hbase/` | Table loaders, query script, shell commands (`schema_and_queries.txt`) |
| `spark/` | Schemas, quality check, cleaning job, cohort analysis, Spark SQL, integrated funnel |
| `viz/` | Chart scripts; images in `viz/figures/` |
| `results/` | Small CSV outputs of the Spark jobs |
| `report/` | Technical report (PDF) |

## Prerequisites

- Docker Desktop and Python 3.10+
- `pip install -r requirements.txt`
- Ports used on the host: MongoDB **27018** (a local Windows MongoDB may already use 27017),
  HBase Thrift 9090, HBase web UI 16010, Spark UI 4040, Jupyter 8888.

## Reproduce the results

Run every command from the project root.

**1. Start the stack** (containers restart automatically after a reboot)

```
docker compose up -d
```

HBase needs about a minute before it accepts connections.

**2. Generate the data** (defaults: 10,000 users, 5,000 products, 25 categories,
50,000 transactions, 200,000 sessions; the `data/` folder is not committed to Git)

```
python scripts/dataset_generator.py --out data
```

**3. Part 1: MongoDB**

```
python mongo/load_mongo.py          # loads categories, products, transactions, users, 100,000 sessions, indexes
python mongo/aggregations.py        # top products, weekly revenue by category, user segments
```

**4. Part 1: HBase**

```
docker exec -it ecom-hbase /opt/hbase/bin/hbase shell   # then paste the 'create' lines from hbase/schema_and_queries.txt
python hbase/load_hbase.py                              # user_sessions: 100,000 rows, key user_id#reverse_ts#session_id
python hbase/load_product_daily.py                      # product_daily: key product_id#yyyymmdd
python hbase/query_hbase.py --user user_007129          # latest sessions, date range, row count
```

(`load_hbase.py` recreates its own table, so the shell `create` step is only needed for `product_daily`
if you skip the loader. Sample shell queries are in `hbase/schema_and_queries.txt`.)

**5. Part 2: Spark** (jobs run inside the `ecom-spark` container; the project folder is mounted at `/home/jovyan/work`)

```
docker exec ecom-spark spark-submit --driver-memory 4g /home/jovyan/work/spark/03_quality_check.py
docker exec ecom-spark spark-submit --driver-memory 4g /home/jovyan/work/spark/04_clean.py
docker exec ecom-spark spark-submit --driver-memory 2g /home/jovyan/work/spark/05_cohort.py
docker exec ecom-spark spark-submit --driver-memory 2g /home/jovyan/work/spark/06_sql.py
```

**6. Part 3: integrated funnel** (Spark reads HBase via `happybase` and MongoDB via `pymongo`)

```
docker exec ecom-spark pip install pymongo happybase
docker exec ecom-spark spark-submit --driver-memory 3g /home/jovyan/work/spark/08_funnel.py
```

**7. Part 4: charts** (run on the host)

```
python viz/01_funnel_chart.py
python viz/02_cohort_heatmap.py
python viz/03_weekly_revenue.py
python viz/04_user_segments.py
```

## Key results (generated dataset, 4 Aug to 2 Nov 2026)

- **Funnel (100,000 sessions):** 98.7% view a product, 64.9% add to cart, 29.0% reach checkout,
  20.2% purchase. The largest leak is cart to checkout (only 44.7% continue).
- **Segments:** loyal buyers (7+ orders) are 14.9% of users but 27.0% of revenue.
- **Weekly revenue** is stable at about 3.5M per full week; the **cohort analysis** shows no
  difference in spending between registration months (about 50 per user per day).
- **Engagement:** users in the top session-count quartile place 5.1 orders on average versus 3.7 in the bottom quartile.

## Data notes and limitations

- The data is synthetic: referrer and device are uniformly distributed, so no segment differences
  should be interpreted as behaviour.
- Revenue counts orders with status `completed`, `shipped` or `delivered` (not `processing`).
- About half of all transactions have no `session_id`, and the generator reached its transaction
  cap partway through the second session file; HBase and the funnel use the first 100,000 sessions.
- Products have no subcategory in the generated data, so profit margin cannot be analysed per product.