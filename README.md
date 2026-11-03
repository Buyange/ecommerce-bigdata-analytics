# Distributed Multi-Model Analytics for E-commerce Data
AUCA Big Data Analytics Final Project (individual) - MongoDB + HBase + Apache Spark

## Repository layout
| Folder | Contents |
|---|---|
| `scripts/` | `dataset_generator.py` (configurable synthetic data generator) |
| `mongo/` | MongoDB schema design, loaders, aggregation pipelines |
| `hbase/` | HBase table creation commands, loaders, query examples |
| `spark/` | PySpark batch jobs, Spark SQL, integrated analytics |
| `viz/` | Visualization scripts and exported figures |
| `report/` | Technical report (PDF) |

## Quick start
```bash
# 1. Start MongoDB, HBase and Spark/Jupyter
docker compose up -d

# 2. Generate data (laptop-friendly defaults: 200k sessions, 50k transactions)
pip install -r requirements.txt
python scripts/dataset_generator.py --out data
#   smaller:  python scripts/dataset_generator.py --sessions 50000 --transactions 10000
#   full:     python scripts/dataset_generator.py --sessions 2000000 --transactions 500000

# 3. Service UIs
#    HBase master  http://localhost:16010
#    Spark UI      http://localhost:4040
#    Jupyter       http://localhost:8888  (token: docker logs ecom-spark)
```

## Reproducing results
_To be filled in as each part is completed (Part 1 -> Part 4)._
