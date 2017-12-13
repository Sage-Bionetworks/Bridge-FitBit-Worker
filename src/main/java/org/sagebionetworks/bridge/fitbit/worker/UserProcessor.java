package org.sagebionetworks.bridge.fitbit.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.client.fluent.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.fitbit.bridge.FitBitUser;
import org.sagebionetworks.bridge.fitbit.schema.ColumnSchema;
import org.sagebionetworks.bridge.fitbit.schema.EndpointSchema;
import org.sagebionetworks.bridge.fitbit.schema.TableSchema;
import org.sagebionetworks.bridge.fitbit.schema.UrlParameterType;
import org.sagebionetworks.bridge.fitbit.util.Utils;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;

/** The User Processor downloads data from the FitBit Web API and collates the data into tables. */
public class UserProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(UserProcessor.class);

    /** Processes the given endpoint for the given user. This is the main entry point into the User Processor. */
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
        String response = makeHttpRequest(url, user.getAccessToken());
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
                    warnWrapper("Table " + tableId + " is neither array nor object for user " +
                            user.getHealthCode());
                }
            } else if (!endpointSchema.getIgnoredKeys().contains(oneResponseKey)) {
                warnWrapper("Unexpected table " + tableId + " for user " + user.getHealthCode());
            }
        }
    }

    // Helper to process a single row of FitBit data.
    private void processTableRowForUser(RequestContext ctx, FitBitUser user, EndpointSchema endpointSchema,
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
                warnWrapper("Unexpected column " + oneColumnName + " in table " + tableId + " for user " +
                        user.getHealthCode());
            } else {
                Object value = Utils.serializeJsonForColumn(columnValueNode, columnSchema);
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

    // Abstracts away the HTTP call to FitBit Web API.
    // Visible for testing.
    String makeHttpRequest(String url, String accessToken) throws IOException {
        return Request.Get(url).setHeader("Authorization", "Bearer " + accessToken).execute()
                .returnContent().asString();
    }

    // Warn wrapper, so that we can use mocks and spies to verify that we're handling unusual cases.
    // Visible for testing
    void warnWrapper(String msg) {
        LOG.warn(msg);
    }
}
