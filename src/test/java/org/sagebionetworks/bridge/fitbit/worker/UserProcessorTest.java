package org.sagebionetworks.bridge.fitbit.worker;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.fitbit.bridge.FitBitUser;
import org.sagebionetworks.bridge.fitbit.schema.ColumnSchema;
import org.sagebionetworks.bridge.fitbit.schema.EndpointSchema;
import org.sagebionetworks.bridge.fitbit.schema.TableSchema;
import org.sagebionetworks.bridge.fitbit.schema.UrlParameterType;
import org.sagebionetworks.bridge.rest.model.Study;

public class UserProcessorTest {
    private static final String COLUMN_ID = "my-column";
    private static final String DATE_STRING = "2017-12-12";
    private static final String ENDPOINT_ID = "my-endpoint";
    private static final String IGNORED_KEY = "ignored-key";
    private static final String TABLE_KEY = "table-key";
    private static final String URL = "http://example.com/users/my-user/date/2017-12-12";
    private static final String URL_PATTERN = "http://example.com/users/%s/date/%s";

    private static final String STUDY_ID = "my-study";
    private static final Study STUDY = new Study().identifier(STUDY_ID);

    private static final String TABLE_ID = ENDPOINT_ID + '.' + TABLE_KEY;

    private static final String ACCESS_TOKEN = "my-access-token";
    private static final String HEALTH_CODE = "my-health-code";
    private static final String USER_ID = "my-user";
    private static final FitBitUser USER = new FitBitUser.Builder().withAccessToken(ACCESS_TOKEN)
            .withHealthCode(HEALTH_CODE).withUserId(USER_ID).build();

    private static final TableSchema TABLE_SCHEMA;
    private static final EndpointSchema ENDPOINT_SCHEMA;
    static {
        ColumnSchema columnSchema = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
                .withColumnType(ColumnType.STRING).withMaxLength(128).build();
        TABLE_SCHEMA = new TableSchema.Builder().withTableKey(TABLE_KEY)
                .withColumns(ImmutableList.of(columnSchema)).build();
        ENDPOINT_SCHEMA = new EndpointSchema.Builder().withEndpointId(ENDPOINT_ID)
                .withIgnoredKeys(ImmutableSet.of(IGNORED_KEY)).withUrl(URL_PATTERN)
                .withUrlParameters(ImmutableList.of(UrlParameterType.USER_ID, UrlParameterType.DATE))
                .withTables(ImmutableList.of(TABLE_SCHEMA)).build();
    }

    private RequestContext ctx;
    private String mockHttpResponse;
    private UserProcessor processor;

    @BeforeMethod
    public void setup() throws Exception {
        // Reset mockHttpResponse, because sometimes TestNG doesn't.
        mockHttpResponse = null;

        // Spy processor so we can mock out the rest call.
        processor = spy(new UserProcessor());

        // Use a doAnswer(), so the tests can specify mockHttpResponse. The tests will also use verify() to validate
        // input args.
        doAnswer(invocation -> mockHttpResponse).when(processor).makeHttpRequest(any(), any());

        // Make request context.
        ctx = new RequestContext(DATE_STRING, STUDY, mock(File.class));
    }

    @Test
    public void normalCaseObjectTable() throws Exception {
        // Make HTTP response.
        mockHttpResponse = "{\n" +
                "   \"" + TABLE_KEY + "\":{\n" +
                "       \"" + COLUMN_ID + "\":\"Just one value\"\n" +
                "   }\n" +
                "}";

        // Execute and validate
        processor.processEndpointForUser(ctx, USER, ENDPOINT_SCHEMA);

        List<Map<String, String>> rowList = validatePopulatedTablesById();
        assertEquals(rowList.size(), 1);
        validateRow(rowList.get(0), "Just one value");

        verify(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        verify(processor, never()).warnWrapper(any());
    }

    @Test
    public void normalCaseArrayTable() throws Exception {
        // Make HTTP response.
        mockHttpResponse = "{\n" +
                "   \"" + TABLE_KEY + "\":[\n" +
                "       {\"" + COLUMN_ID + "\":\"foo\"},\n" +
                "       {\"" + COLUMN_ID + "\":\"bar\"},\n" +
                "       {\"" + COLUMN_ID + "\":\"baz\"}\n" +
                "   ]\n" +
                "}";

        // Execute and validate
        processor.processEndpointForUser(ctx, USER, ENDPOINT_SCHEMA);

        List<Map<String, String>> rowList = validatePopulatedTablesById();
        assertEquals(rowList.size(), 3);
        validateRow(rowList.get(0), "foo");
        validateRow(rowList.get(1), "bar");
        validateRow(rowList.get(2), "baz");

        verify(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        verify(processor, never()).warnWrapper(any());
    }

    @Test
    public void normalCaseContextAlreadyHasTable() throws Exception {
        // Set up context with previous user's data.
        Map<String, String> previousUsersRowMap = ImmutableMap.<String, String>builder()
                .put(Constants.COLUMN_HEALTH_CODE, "previous user's health code")
                .put(Constants.COLUMN_CREATED_DATE, DATE_STRING)
                .put(COLUMN_ID, "previous user's data")
                .build();

        PopulatedTable populatedTable = new PopulatedTable(TABLE_ID, TABLE_SCHEMA);
        populatedTable.getRowList().add(previousUsersRowMap);

        ctx.getPopulatedTablesById().put(TABLE_ID, populatedTable);

        // Make HTTP response.
        mockHttpResponse = "{\n" +
                "   \"" + TABLE_KEY + "\":{\n" +
                "       \"" + COLUMN_ID + "\":\"current user's data\"\n" +
                "   }\n" +
                "}";

        // Execute and validate
        processor.processEndpointForUser(ctx, USER, ENDPOINT_SCHEMA);

        List<Map<String, String>> rowList = validatePopulatedTablesById();
        assertEquals(rowList.size(), 2);
        assertEquals(rowList.get(0), previousUsersRowMap);
        validateRow(rowList.get(1), "current user's data");

        verify(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        verify(processor, never()).warnWrapper(any());
    }

    @Test
    public void edgeCaseUnexpectedTableKey() throws Exception {
        // Make HTTP response.
        mockHttpResponse = "{\n" +
                "   \"wrong-table-key\":{\n" +
                "       \"any-column\":\"Any value\"\n" +
                "   }\n" +
                "}";

        // Execute and validate
        processor.processEndpointForUser(ctx, USER, ENDPOINT_SCHEMA);
        assertTrue(ctx.getPopulatedTablesById().isEmpty());
        verify(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        verify(processor).warnWrapper("Unexpected table " + ENDPOINT_ID + ".wrong-table-key for user " +
                HEALTH_CODE);
    }

    @Test
    public void edgeCaseIgnoredKey() throws Exception {
        // Make HTTP response.
        mockHttpResponse = "{\n" +
                "   \"" + IGNORED_KEY + "\":{\n" +
                "       \"any-column\":\"Any value\"\n" +
                "   }\n" +
                "}";

        // Execute and validate
        processor.processEndpointForUser(ctx, USER, ENDPOINT_SCHEMA);
        assertTrue(ctx.getPopulatedTablesById().isEmpty());
        verify(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        verify(processor, never()).warnWrapper(any());
    }

    @Test
    public void edgeCaseNeitherArrayNorObject() throws Exception {
        // Make HTTP response.
        mockHttpResponse = "{\n" +
                "   \"" + TABLE_KEY + "\":\"This is an invalid data type\"\n" +
                "}";

        // Execute and validate
        processor.processEndpointForUser(ctx, USER, ENDPOINT_SCHEMA);

        List<Map<String, String>> rowList = validatePopulatedTablesById();
        assertTrue(rowList.isEmpty());

        verify(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        verify(processor).warnWrapper("Table " + TABLE_ID + " is neither array nor object for user " +
                HEALTH_CODE);
    }

    @Test
    public void edgeCaseUnexpectedColumnInTable() throws Exception {
        // Make HTTP response.
        mockHttpResponse = "{\n" +
                "   \"" + TABLE_KEY + "\":{\n" +
                "       \"wrong-column\":\"value is ignored\"\n" +
                "   }\n" +
                "}";

        // Execute and validate
        processor.processEndpointForUser(ctx, USER, ENDPOINT_SCHEMA);

        List<Map<String, String>> rowList = validatePopulatedTablesById();
        assertTrue(rowList.isEmpty());

        verify(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        verify(processor).warnWrapper("Unexpected column wrong-column in table " + TABLE_ID + " for user " +
                HEALTH_CODE);
    }

    @Test
    public void edgeCaseEmptyRow() throws Exception {
        // Make HTTP response.
        mockHttpResponse = "{\n" +
                "   \"" + TABLE_KEY + "\":{}\n" +
                "}";

        // Execute and validate
        processor.processEndpointForUser(ctx, USER, ENDPOINT_SCHEMA);

        List<Map<String, String>> rowList = validatePopulatedTablesById();
        assertTrue(rowList.isEmpty());

        verify(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        verify(processor, never()).warnWrapper(any());
    }

    @Test
    public void edgeCaseNullValue() throws Exception {
        // Make HTTP response.
        mockHttpResponse = "{\n" +
                "   \"" + TABLE_KEY + "\":{\n" +
                "       \"" + COLUMN_ID + "\":null\n" +
                "   }\n" +
                "}";

        // Execute and validate
        processor.processEndpointForUser(ctx, USER, ENDPOINT_SCHEMA);

        List<Map<String, String>> rowList = validatePopulatedTablesById();
        assertTrue(rowList.isEmpty());

        verify(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        verify(processor, never()).warnWrapper(any());
    }

    // Validate the PopulatedTablesById is correct, and returns the row list.
    private List<Map<String, String>> validatePopulatedTablesById() {
        Map<String, PopulatedTable> populatedTablesById = ctx.getPopulatedTablesById();
        assertEquals(populatedTablesById.size(), 1);

        PopulatedTable populatedTable = populatedTablesById.get(TABLE_ID);
        assertEquals(populatedTable.getTableId(), TABLE_ID);
        assertEquals(populatedTable.getTableSchema(), TABLE_SCHEMA);

        return populatedTable.getRowList();
    }

    private static void validateRow(Map<String, String> rowValueMap, String expected) {
        assertEquals(rowValueMap.size(), 3);
        assertEquals(rowValueMap.get(Constants.COLUMN_HEALTH_CODE), HEALTH_CODE);
        assertEquals(rowValueMap.get(Constants.COLUMN_CREATED_DATE), DATE_STRING);
        assertEquals(rowValueMap.get(COLUMN_ID), expected);
    }
}
