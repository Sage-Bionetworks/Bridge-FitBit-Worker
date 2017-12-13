package org.sagebionetworks.bridge.fitbit.util;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.fitbit.schema.ColumnSchema;
import org.sagebionetworks.bridge.fitbit.worker.Constants;
import org.sagebionetworks.bridge.fitbit.worker.PopulatedTable;
import org.sagebionetworks.bridge.rest.model.Study;

/** Utility functions */
public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    private static final Joiner JOINER_COLUMN_JOINER = Joiner.on('\t').useForNull("");

    // Every table has a healthCode (guid) and createdOn (YYYY-MM-DD) column.
    // Visible for testing
    static final List<ColumnSchema> COMMON_COLUMN_LIST = ImmutableList.of(
            new ColumnSchema.Builder().withColumnId(Constants.COLUMN_HEALTH_CODE).withColumnType(ColumnType.STRING)
                    .withMaxLength(36).build(),
            new ColumnSchema.Builder().withColumnId(Constants.COLUMN_CREATED_DATE).withColumnType(ColumnType.STRING)
                    .withMaxLength(10).build());

    /**
     * Helper method which merges the common column list with the table-specific column schemas and returns the full
     * list of table columns.
     */
    public static List<ColumnSchema> getAllColumnsForTable(PopulatedTable table) {
        // Combine common columns with table-specific columns.
        List<ColumnSchema> allColumnList = new ArrayList<>();
        allColumnList.addAll(COMMON_COLUMN_LIST);
        allColumnList.addAll(table.getTableSchema().getColumns());
        return allColumnList;
    }

    /** Helper method which converts a ColumnSchema to a Synapse ColumnModel. */
    public static ColumnModel getColumnModelForSchema(ColumnSchema schema) {
        // Column ID in the schema is column name in the model.
        ColumnModel columnModel = new ColumnModel();
        columnModel.setName(schema.getColumnId());
        columnModel.setColumnType(schema.getColumnType());
        if (schema.getMaxLength() != null) {
            columnModel.setMaximumSize(schema.getMaxLength().longValue());
        }
        return columnModel;
    }

    /**
     * Returns true if the study is configured for FitBit data export. This means that the study is configured to
     * export to Synapse (has the synapseProjectId and synapseDataAccessTeamId properties) and is configured for FitBit
     * OAuth (has "fitbit" in its oAuthProviders).
     */
    public static boolean isStudyConfigured(Study study) {
        return study.getSynapseProjectId() != null
                && study.getSynapseDataAccessTeamId() != null
                && study.getOAuthProviders() != null
                && study.getOAuthProviders().containsKey(Constants.FITBIT_VENDOR_ID);
    }

    /** Helper method to serialize a JsonNode to write to the given Column. */
    public static String serializeJsonForColumn(JsonNode node, ColumnSchema columnSchema) {
        String columnId = columnSchema.getColumnId();

        // Short-cut: null check.
        if (node == null || node.isNull()) {
            return null;
        }

        // Canonicalize into an object.
        Object value = null;
        switch (columnSchema.getColumnType()) {
            case BOOLEAN:
                if (node.isBoolean()) {
                    value = node.booleanValue();
                } else {
                    LOG.warn("Expected boolean for column " + columnId + ", got " + node.getNodeType().name());
                }
                break;
            case DATE:
                // Currently, all dates from FitBit web API are in UTC, so we can just use epoch milliseconds,
                // which is what Synapse expects anyway.
                if (node.isTextual()) {
                    String dateTimeStr = node.textValue();
                    try {
                        value = DateTime.parse(dateTimeStr).getMillis();
                    } catch (IllegalArgumentException ex) {
                        LOG.warn("Invalid DateTime format " + dateTimeStr);
                    }
                } else {
                    LOG.warn("Expected string for column " + columnId + ", got " + node.getNodeType().name());
                }
                break;
            case DOUBLE:
                if (node.isNumber()) {
                    value = node.decimalValue().toPlainString();
                } else {
                    LOG.warn("Expected number for column " + columnId + ", got " + node.getNodeType().name());
                }
                break;
            case INTEGER:
                if (node.isNumber()) {
                    value = node.longValue();
                } else {
                    LOG.warn("Expected number for column " + columnId + ", got " + node.getNodeType().name());
                }
                break;
            case LARGETEXT:
                // LargeText is used for when the value is an array or an object. In this case, we want to
                // write the JSON verbatim to Synapse.
                value = node;
                break;
            case STRING:
                String textValue;
                if (node.isTextual()) {
                    textValue = node.textValue();
                } else {
                    textValue = node.toString();
                }

                // Strings have a max length. If the string is too long, truncate it.
                int valueLength = textValue.length();
                int maxLength = columnSchema.getMaxLength();
                if (valueLength > maxLength) {
                    LOG.warn("Truncating value of length " + valueLength + " to max length " + maxLength +
                            " for column " + columnId);
                    textValue = textValue.substring(0, maxLength);
                }
                value = textValue;
                break;
            default:
                LOG.warn("Unexpected type " + columnSchema.getColumnType().name() + " for column " + columnId);
                break;
        }

        // If the canonicalized value is null (possibly because of type errors), return null instead of converting to
        // a string.
        if (value == null) {
            return null;
        }

        // Convert to string.
        return String.valueOf(value);
    }

    /** Helper method, which formats and writes a row of values (represented as a String List) to the given Writer. */
    public static void writeRowToTsv(PrintWriter tsvWriter, List<String> rowValueList) {
        tsvWriter.println(JOINER_COLUMN_JOINER.join(rowValueList));
    }
}
