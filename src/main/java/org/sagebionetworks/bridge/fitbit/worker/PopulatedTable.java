package org.sagebionetworks.bridge.fitbit.worker;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.sagebionetworks.bridge.fitbit.schema.TableSchema;

// todo doc
public class PopulatedTable {
    private final String tableId;
    private final TableSchema tableSchema;
    private final List<Map<String, String>> rowList = new ArrayList<>();
    private File tsvFile;

    public PopulatedTable(String tableId, TableSchema tableSchema) {
        this.tableId = tableId;
        this.tableSchema = tableSchema;
    }

    public String getTableId() {
        return tableId;
    }

    public TableSchema getTableSchema() {
        return tableSchema;
    }

    public List<Map<String, String>> getRowList() {
        return rowList;
    }

    public File getTsvFile() {
        return tsvFile;
    }

    public void setTsvFile(File tsvFile) {
        this.tsvFile = tsvFile;
    }
}
