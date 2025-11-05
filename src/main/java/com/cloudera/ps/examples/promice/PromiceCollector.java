package com.cloudera.ps.examples.promice;

import java.util.HashMap;
import java.util.Map;
import java.lang.NumberFormatException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.hive.HiveCatalog;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;

public class PromiceCollector {
    final static int METRICS_PORT_DEFAULT = 9998;

    private static PromiceCollector INSTANCE;

    HiveCatalog catalog;
    HTTPServer promServer;

    private static final Gauge TOTAL_RECORDS = Gauge.build()
            .name("total_records")
            .help("Total records from the current table snapshot")
            .labelNames("database", "table")
            .register();

    private static final Gauge TOTAL_FILES_SIZE = Gauge.build()
            .name("total_files_size")
            .help("Total file size from the current table snapshot")
            .labelNames("database", "table")
            .register();

    private static final Gauge TOTAL_DATA_FILES = Gauge.build()
            .name("total_data_files")
            .help("Total data file count from the current table snapshot")
            .labelNames("database", "table")
            .register();

    private static final Gauge TOTAL_DELETE_FILES = Gauge.build()
            .name("total_delete_files")
            .help("Total delete file count from the current table snapshot")
            .labelNames("database", "table")
            .register();

    private PromiceCollector(Configuration conf) throws IOException {
        catalog = new HiveCatalog();
        catalog.setConf(conf);
        catalog.initialize("hive", new HashMap());

        promServer = new HTTPServer(METRICS_PORT_DEFAULT);
    }

    public static synchronized PromiceCollector getInstance(Configuration conf) throws IOException {
        if(INSTANCE == null) {
            INSTANCE = new PromiceCollector(conf);
        }

        return INSTANCE;
    }

    private void setGauge(String databaseName, String tableName, Map<String, String> summary, String metric, Gauge guage) {
        if (summary != null && summary.containsKey(metric)) {
            String valueStr = summary.get(metric);

            try {
                long value = Long.valueOf(valueStr);

                guage.labels(databaseName, tableName).set(value);
            } catch (NumberFormatException e) {}
        }
    }

    public void collect(String databaseName, String tableName) {
        TableIdentifier identifier = TableIdentifier.of(databaseName, tableName);
        Table table = catalog.loadTable(identifier);

        Snapshot currentSnapshot = table.currentSnapshot();

        if (currentSnapshot != null) {
            System.out.println("Iceberg snapshot [" + currentSnapshot.snapshotId() + "]: " + currentSnapshot.summary());

            Map<String, String> summary = currentSnapshot.summary();

            setGauge(databaseName, tableName, summary, "total-records", TOTAL_RECORDS);
            setGauge(databaseName, tableName, summary, "total-files-size", TOTAL_FILES_SIZE);
            setGauge(databaseName, tableName, summary, "total-data-files", TOTAL_DATA_FILES);
            setGauge(databaseName, tableName, summary, "total-delete-files", TOTAL_DELETE_FILES);
        }
    }
}
