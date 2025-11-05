package com.cloudera.ps.examples.promice;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.Table;

public class PromiceListener extends MetaStoreEventListener {
    PromiceCollector collector;

    public PromiceListener(Configuration conf) throws IOException {
        super(conf);

        collector = PromiceCollector.getInstance(conf);
    }

    @Override
    public void onAlterTable(AlterTableEvent event) {
        Table table = event.getNewTable();
        String storageHandler = table.getParameters().get("storage_handler");

        if (storageHandler != null && storageHandler.contains("IcebergStorageHandler")) {
            collector.collect(table.getDbName(), table.getTableName());
        }
    }
}
