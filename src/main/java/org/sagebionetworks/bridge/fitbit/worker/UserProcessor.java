package org.sagebionetworks.bridge.fitbit.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.client.fluent.Request;
import org.joda.time.DateTime;

import org.sagebionetworks.bridge.fitbit.bridge.FitBitUser;
import org.sagebionetworks.bridge.fitbit.schema.ColumnSchema;
import org.sagebionetworks.bridge.fitbit.schema.EndpointSchema;
import org.sagebionetworks.bridge.fitbit.schema.TableSchema;
import org.sagebionetworks.bridge.fitbit.schema.UrlParameterType;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;

public class UserProcessor {
    // todo refactor
    public void processEndpointForUser(RequestContext ctx, FitBitUser user, EndpointSchema endpointSchema)
            throws IOException {
        // Generate url parameters
        List<String> resolvedUrlParamList = new ArrayList<>();
        for (UrlParameterType oneUrlParam : endpointSchema.getUrlParameters()) {
            switch (oneUrlParam) {
                case DATE:
                    resolvedUrlParamList.add(ctx.getDate());
                    break;
                case USER_ID:
                    resolvedUrlParamList.add(user.getUserId());
                    break;
            }
        }

        // Get data from FitBit
        String url = String.format(endpointSchema.getUrl(), resolvedUrlParamList.toArray());
        String response = Request.Get(url)
                .setHeader("Authorization", "Bearer " + user.getAccessToken()).execute()
                .returnContent().asString();
        JsonNode responseNode = DefaultObjectMapper.INSTANCE.readTree(response);

        // Process each key (top-level table) in the response
        Iterator<String> responseKeyIter = responseNode.fieldNames();
        while (responseKeyIter.hasNext()) {
            String oneResponseKey = responseKeyIter.next();
            String tableId = endpointSchema.getEndpointId() + '.' + oneResponseKey;
            JsonNode dataNode = responseNode.get(oneResponseKey);

            TableSchema oneTableSchema = endpointSchema.getTablesByKey().get(oneResponseKey);
            if (oneTableSchema != null) {
                ctx.getPopulatedTablesById().computeIfAbsent(tableId, key -> new PopulatedTable(tableId,
                        oneTableSchema));

                if (dataNode.isArray()) {
                    // dataNode is a list of rows
                    for (JsonNode rowNode : dataNode) {
                        processTableRowForUser(ctx, user, endpointSchema, oneTableSchema, rowNode);
                    }
                } else if (dataNode.isObject()) {
                    // The object is the row we need to process.
                    processTableRowForUser(ctx, user, endpointSchema, oneTableSchema, dataNode);
                } else {
                    // todo warn
                }
            } else if (!endpointSchema.getIgnoredKeys().contains(oneResponseKey)) {
                // todo warn about unexpected key
            }
        }
    }

    // todo refactor
    public void processTableRowForUser(RequestContext ctx, FitBitUser user, EndpointSchema endpointSchema,
            TableSchema tableSchema, JsonNode rowNode) {
        String tableId = endpointSchema.getEndpointId() + '.' + tableSchema.getTableKey();
        PopulatedTable populatedTable = ctx.getPopulatedTablesById().get(tableId);
        Map<String, String> rowValueMap = new HashMap<>();

        // Iterate through all values in the node. Serialize the values into the PopulatedTable.
        Iterator<String> columnNameIter = rowNode.fieldNames();
        while (columnNameIter.hasNext()) {
            String oneColumnName = columnNameIter.next();
            JsonNode columnValueNode = rowNode.get(oneColumnName);

            ColumnSchema columnSchema = tableSchema.getColumnsById().get(oneColumnName);
            if (columnSchema == null) {
                // todo warn
            } else {
                Object value = null;
                switch (columnSchema.getColumnType()) {
                    case BOOLEAN:
                        value = columnValueNode.booleanValue();
                        break;
                    case DATE:
                        // Currently, all dates from FitBit web API are in UTC, so we can just use epoch milliseconds,
                        // which is what Synapse expects anyway.
                        String dateTimeStr = columnValueNode.textValue();
                        value = DateTime.parse(dateTimeStr).getMillis();
                        break;
                    case DOUBLE:
                        value = columnValueNode.decimalValue().toPlainString();
                        break;
                    case INTEGER:
                        value = columnValueNode.longValue();
                        break;
                    case LARGETEXT:
                        // LargeText is used for when the value is an array or an object. In this case, we want to
                        // write the JSON verbatim to Synapse.
                        value = columnValueNode;
                        break;
                    case STRING:
                        // Strings have a max length. If the string is too long, truncate it.
                        String textValue = columnValueNode.textValue();
                        if (textValue.length() > columnSchema.getMaxLength()) {
                            // todo warn
                            textValue = textValue.substring(0, columnSchema.getMaxLength());
                        }
                        value = textValue;
                        break;
                    default:
                        // todo warn
                        break;
                }

                if (value != null) {
                    rowValueMap.put(oneColumnName, value.toString());
                }
            }
        }

        if (!rowValueMap.isEmpty()) {
            // Always include the user's health code and the created date.
            rowValueMap.put(Constants.COLUMN_HEALTH_CODE, user.getHealthCode());
            rowValueMap.put(Constants.COLUMN_CREATED_DATE, ctx.getDate());

            // Add the row to the table
            populatedTable.getRowList().add(rowValueMap);
        }
    }
}
