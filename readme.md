# Promice

A Hive metastore listener that populates a Prometheus metrics endpoint with Iceberg snapshot statistics.

Currently listens to port 9998 on the Hive metastore server (no TLS and no auth).

Provides the following metrics (labeled with database and table):

- _total_records_
- _total_files_size_
- _total_data_files_
- _total_delete_files_
