# mongo-cdc-kafka-parquet
# MongoDB CDC to Kafka to Parquet Pipeline

## Overview

This project implements a Change Data Capture (CDC) pipeline that streams real-time data changes from a MongoDB collection into Kafka, and then consumes those messages in batches to save them as Parquet files. This setup is useful for building scalable, event-driven data architectures for analytics and processing.

---

## Architecture

```plaintext
MongoDB (Change Streams) → Python Producer → Kafka → Python Consumer → Parquet Files
