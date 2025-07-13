# ACME Delivery Monitoring System – Interview Project

## Consideration
This project is a monitoring solution built as part of an interview task for **ACME Delivery Services**. The goal was to create a system that could provide near real-time insights into order data — helping to answer questions like “How many open orders are there?” or “Who are our top customers?”

I explored both **streaming** and **batch** processing approaches, aiming to balance reliability, latency, and simplicity. Below is a walkthrough of my thought process, architecture choices, challenges, and what worked (and what didn’t).

In the beginning I was thinking to do this in Apache Spark because it supports Python, a language that is simpler and easier to deploy, perfect for interview task like this, as opposed to Flink. It mainly supports Java and its Python implementation is not great. However, Spark doesn't come with a scheduler on itself so it only processes the table once and the docker dies. I read that Debezium+kafka can solve this issue by feeding data to Spark which Spark listens to Kafka. Despite my reluctant to try a completely new tool I never tried before I took the risk. I managed to connect Debezium and Kafka, make Spark ingest the data coming from the source table on my local computer and output the reult in `monitoring.open_orders`. However, there is an issue with the calculation.

---
## Data model
The output tables can be found in `db-scripts/ddm_monitoring.sql`

---
## Streaming pipeline with Debezium and Kafka
Branch: `streaming-solution`

For monitoring purposes, having fresh data is important — but **perfect real-time** isn’t necessary. A delay of a couple of minutes is totally fine.

As I mentioned before, I went with **Apache Spark Structured Streaming**, which works great with Python, is easier to set up in a Dockerized environment, and fit the scope of the task really well.

### How to run the pipeline
I had issues dockerize the Spark app (see issues in commits). Therefore, there are some commands I needed to run to trigger the application. 
- Check out to branch `streaming-solution`
- Make sure you have spark older than 4.0.0
- Run
```bash
docker-compose build
```
- In root project run 
```bash
- spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4,io.delta:delta-core_2.12:2.4.0 \
  --jars jar/postgresql-42.7.3.jar \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  etl/spark_cdc_job.py
  ```

### Architecture Overview

Here’s a quick look at how everything fits together:

PostgreSQL → Debezium → Kafka → Apache Spark → Delta Table

1. **PostgreSQL** holds the raw transactional order data.
2. **Debezium** captures changes in the database (CDC).
3. **Kafka** streams those changes to consumers.
4. **Apache Spark** listens to Kafka, processes the data, and writes it into a **Delta table**.
5. The Delta table (`monitoring.open_orders`) reflects the latest state of each order.

---

### 🔁 How It Works

- New data arrives in small batches.
- Spark uses **windowing logic** to filter out older duplicates and only keep the latest change per `order_id`.
- Based on the `updated_at` timestamp:
    - If it’s a new order → insert it
    - If it’s newer than the current record → update it
    - If it’s older → skip it (probably late-arriving data)

This ensures the Delta table always reflects the most recent known status of each order.

---

### 🐞 What Went Wrong

#### 🔺 Pending Order Count Seems Too High

After reviewing the results, I noticed the number of open/pending orders was much higher than expected. After quite a bit of debugging, I figured out the root cause:

- When Debezium captures the initial state of the database, it sends *all* the existing rows — many of which are older and show orders as still pending.
- In some cases, my deduplication logic didn’t correctly identify the **latest** update per order.

So, the monitoring table was sometimes showing an outdated view of those orders. I didn’t have enough time to fully resolve this — but I now understand how I’d fix it (probably with a more reliable event timestamp strategy).

---

## 🧪 The Batch Backup Plan

Branch: `non-cdc-solution`

When I got stuck troubleshooting the streaming version, I decided to try a **batch pipeline** as a fallback. It reads the full table, processes it once, and outputs the results.

✅ Calculating Number of open orders by DELIVERY_DATE and STATUS worked perfectly — the results were accurate and output in monitoring.open_orders.  
❌ The challenge? Spark doesn’t come with a scheduler by default. I tried using `cron`, but ran into Docker permission issues I couldn’t fully resolve in time.

### How to run the pipeline
I had issues dockerize the Spark app (see issues in commits). Therefore, there are some commands I needed to run to trigger the application.
- Check out to branch `non-cdc-solution`
- Run
```bash
docker-compose build
```
- To run on local make sure to have `postgresql-42.7.3.jar` under jar and run
```bash
- spark-submit \
  --jars jar/postgresql-42.7.3.jar \
  etl/spark_job.py
  ```
---

## 🛠️ Troubleshooting Log

Some quick notes on the issues I hit and how I addressed them:

### PostgreSQL Volume Permissions

```bash
chmod: changing permissions of '/var/lib/postgresql/data': Operation not permitted
```
Fix:
Defined the volume like this in docker-compose.yml:
```bash
volumes:
- db-data:/var/lib/postgresql/data
```
### Enabling Replication for Debezium
```bash
docker exec -it <container_id> psql -U finance_db_user -d finance_db
ALTER ROLE cdc_user WITH REPLICATION;
```
Then verified with:
```bash
SELECT rolname, rolreplication FROM pg_roles WHERE rolname = 'cdc_user';
SELECT slot_name, active FROM pg_replication_slots WHERE slot_name = 'cdc_pgoutput';
```
### Spark Kafka Source Error
```bash
Failed to find data source: kafka
```
Fix: Installed Spark 3.4.4 and made sure the Kafka package was included via --packages when running the job.


## What would I do if I had more time to finish?
Overall I faced a lot of challenges that required me to much time to troubleshoot at the time of finishing this task. If I had the time 
I would focus on just the streaming solution with Debezium and Kafka because conceptually this provides the functionality that the challenge asks for. i.e. continuous processing for monitoring. The implementation for the other queries would simpler once get this figured out.
- Troubleshoot further to get the correct output for `Number of open orders by DELIVERY_DATE and STATUS`
- In real life I would have the *transformation layer* which would contain the data processing part and the *submission layer* which would serve the clients via API.
- For the query `Top 3 delivery dates with more open orders`, this can be retrieved using the same table `monitoring.open_orders`. To calculate the top 3 we can do this in the API application because it wouldn't need high data processing effort. This still allows us to query historical information along with the current status by filtering using `processed date`.
- For the query `Number of open pending items by PRODUCT_ID`, I would join `orders` and `order_items`
```bash
select product_id, count(*) from operations.orders_items
where order_id = (select order_id from operations.orders where status == 'PENDING')
;
```
- For the query `Top 3 Customers with more pending orders` would use the same way as the point above but with `customer` and `orders` table joined by `customer_id`
- I would build the APIs using FastAPI or Flask and secure it using basic authentication