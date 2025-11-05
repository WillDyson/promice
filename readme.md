# Promice

A Hive metastore listener that populates a Prometheus metrics endpoint with Iceberg snapshot statistics.

Currently listens to port 9998 on the Hive metastore server (no TLS and no auth).

Provides the following metrics (labeled with database and table):

- _total_records_
- _total_files_size_
- _total_data_files_
- _total_delete_files_

## Example

```
hms_host> curl http://localhost:9998/metrics
# HELP total_delete_files Total delete file count from the current table snapshot
# TYPE total_delete_files gauge
total_delete_files{database="default",table="wdyson_1",} 0.0
# HELP total_data_files Total data file count from the current table snapshot
# TYPE total_data_files gauge
total_data_files{database="default",table="wdyson_1",} 4.0
# HELP total_records Total records from the current table snapshot
# TYPE total_records gauge
total_records{database="default",table="wdyson_1",} 4.0
# HELP total_files_size Total file size from the current table snapshot
# TYPE total_files_size gauge
total_files_size{database="default",table="wdyson_1",} 1600.0
```
