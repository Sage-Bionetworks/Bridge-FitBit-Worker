package org.sagebionetworks.bridge.fitbit.schema;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.sagebionetworks.repo.model.table.ColumnType;

// todo doc
@JsonDeserialize(builder = ColumnSchema.Builder.class)
public class ColumnSchema {
    private final String columnId;
    private final ColumnType columnType;
    private final Integer maxLength;

    private ColumnSchema(String columnId, ColumnType columnType, Integer maxLength) {
        this.columnId = columnId;
        this.columnType = columnType;
        this.maxLength = maxLength;
    }

    public String getColumnId() {
        return columnId;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public Integer getMaxLength() {
        return maxLength;
    }

    public static class Builder {
        private String columnId;
        private ColumnType columnType;
        private Integer maxLength;

        public Builder withColumnId(String columnId) {
            this.columnId = columnId;
            return this;
        }

        public Builder withColumnType(ColumnType columnType) {
            this.columnType = columnType;
            return this;
        }

        public Builder withMaxLength(Integer maxLength) {
            this.maxLength = maxLength;
            return this;
        }

        public ColumnSchema build() {
            return new ColumnSchema(columnId, columnType, maxLength);
        }
    }
}
