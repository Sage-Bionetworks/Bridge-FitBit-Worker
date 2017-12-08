package org.sagebionetworks.bridge.fitbit.schema;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Maps;

// todo doc
public class TableSchema {
    private String tableKey;
    private List<ColumnSchema> columns;
    private Map<String, ColumnSchema> columnsById;

    public String getTableKey() {
        return tableKey;
    }

    public void setTableKey(String tableKey) {
        this.tableKey = tableKey;
    }

    public List<ColumnSchema> getColumns() {
        return columns;
    }

    @JsonIgnore
    public Map<String, ColumnSchema> getColumnsById() {
        return columnsById;
    }

    public void setColumns(List<ColumnSchema> columns) {
        this.columns = columns;
        this.columnsById = Maps.uniqueIndex(columns, ColumnSchema::getColumnId);
    }
}
