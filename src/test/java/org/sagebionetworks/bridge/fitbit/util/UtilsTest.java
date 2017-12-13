package org.sagebionetworks.bridge.fitbit.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.fitbit.schema.ColumnSchema;
import org.sagebionetworks.bridge.fitbit.schema.TableSchema;
import org.sagebionetworks.bridge.fitbit.worker.Constants;
import org.sagebionetworks.bridge.fitbit.worker.PopulatedTable;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.OAuthProvider;
import org.sagebionetworks.bridge.rest.model.Study;

public class UtilsTest {
    private static final String COLUMN_ID = "my-column";
    private static final long DATA_ACCESS_TEAM_ID = 1234L;
    private static final String PROJECT_ID = "my-project";
    private static final String TABLE_KEY = "my-table-key";
    private static final String TABLE_ID = "my-table-id";

    private static final ColumnSchema BOOLEAN_COLUMN = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
            .withColumnType(ColumnType.BOOLEAN).build();
    private static final ColumnSchema DATE_COLUMN = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
            .withColumnType(ColumnType.DATE).build();
    private static final ColumnSchema DOUBLE_COLUMN = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
            .withColumnType(ColumnType.DOUBLE).build();
    private static final ColumnSchema INTEGER_COLUMN = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
            .withColumnType(ColumnType.INTEGER).build();
    private static final ColumnSchema LARGETEXT_COLUMN = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
            .withColumnType(ColumnType.LARGETEXT).build();
    private static final ColumnSchema STRING_COLUMN = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
            .withColumnType(ColumnType.STRING).withMaxLength(10).build();
    private static final ColumnSchema UNSUPPORTED_COLUMN = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
            .withColumnType(ColumnType.FILEHANDLEID).build();

    @Test
    public void getAllColumnsForTable() {
        // Make populated table with table schema.
        ColumnSchema myColumnSchema = new ColumnSchema.Builder().withColumnId("my-column")
                .withColumnType(ColumnType.INTEGER).build();
        TableSchema myTableSchema = new TableSchema.Builder().withTableKey(TABLE_KEY)
                .withColumns(ImmutableList.of(myColumnSchema)).build();
        PopulatedTable populatedTable = new PopulatedTable(TABLE_ID, myTableSchema);

        // Execute and validate.
        List<ColumnSchema> allColumnList = Utils.getAllColumnsForTable(populatedTable);
        assertEquals(allColumnList.size(), 3);
        assertEquals(allColumnList.get(0), Utils.COMMON_COLUMN_LIST.get(0));
        assertEquals(allColumnList.get(1), Utils.COMMON_COLUMN_LIST.get(1));
        assertEquals(allColumnList.get(2), myColumnSchema);
    }

    @Test
    public void getColumnModelForSchemaStringType() {
        ColumnSchema columnSchema = new ColumnSchema.Builder().withColumnId("my-column")
                .withColumnType(ColumnType.STRING).withMaxLength(42).build();
        ColumnModel columnModel = Utils.getColumnModelForSchema(columnSchema);
        assertEquals(columnModel.getName(), "my-column");
        assertEquals(columnModel.getColumnType(), ColumnType.STRING);
        assertEquals(columnModel.getMaximumSize().intValue(), 42);
    }

    @Test
    public void getColumnModelForSchemaIntType() {
        ColumnSchema columnSchema = new ColumnSchema.Builder().withColumnId("my-column")
                .withColumnType(ColumnType.INTEGER).build();
        ColumnModel columnModel = Utils.getColumnModelForSchema(columnSchema);
        assertEquals(columnModel.getName(), "my-column");
        assertEquals(columnModel.getColumnType(), ColumnType.INTEGER);
        assertNull(columnModel.getMaximumSize());
    }

    @Test
    public void isConfigured() {
        Study study = new Study().synapseProjectId(PROJECT_ID).synapseDataAccessTeamId(DATA_ACCESS_TEAM_ID)
                .putOAuthProvidersItem(Constants.FITBIT_VENDOR_ID, new OAuthProvider());
        assertTrue(Utils.isStudyConfigured(study));
    }

    @Test
    public void noSynapseProjectId() {
        Study study = new Study().synapseProjectId(null).synapseDataAccessTeamId(DATA_ACCESS_TEAM_ID)
                .putOAuthProvidersItem(Constants.FITBIT_VENDOR_ID, new OAuthProvider());
        assertFalse(Utils.isStudyConfigured(study));
    }

    @Test
    public void noDataAccessTeam() {
        Study study = new Study().synapseProjectId(PROJECT_ID).synapseDataAccessTeamId(null)
                .putOAuthProvidersItem(Constants.FITBIT_VENDOR_ID, new OAuthProvider());
        assertFalse(Utils.isStudyConfigured(study));
    }

    @Test
    public void noOAuthProviders() {
        Study study = new Study().synapseProjectId(PROJECT_ID).synapseDataAccessTeamId(DATA_ACCESS_TEAM_ID)
                .oAuthProviders(null);
        assertFalse(Utils.isStudyConfigured(study));
    }

    @Test
    public void oAuthProvidersDontContainFitBit() {
        Study study = new Study().synapseProjectId(PROJECT_ID).synapseDataAccessTeamId(DATA_ACCESS_TEAM_ID)
                .oAuthProviders(ImmutableMap.of());
        assertFalse(Utils.isStudyConfigured(study));
    }

    @DataProvider(name = "serializeDataProvider")
    public Object[][] serializeDataProvider() {
        return new Object[][] {
                { null, STRING_COLUMN, null },
                { NullNode.instance, STRING_COLUMN, null },
                { new IntNode(1), BOOLEAN_COLUMN, null },
                { new TextNode("true"), BOOLEAN_COLUMN, null },
                { BooleanNode.TRUE, BOOLEAN_COLUMN, "true" },
                { new LongNode(1513152387123L), DATE_COLUMN, null },
                { new TextNode("December 12, 2017 at 18:56:51"), DATE_COLUMN, null },
                { new TextNode("2017-12-12T18:56:51.098Z"), DATE_COLUMN, "1513105011098" },
                { new TextNode("3.14159"), DOUBLE_COLUMN, null },
                { new DecimalNode(new BigDecimal("3.14159")), DOUBLE_COLUMN, "3.14159" },
                { new IntNode(3), DOUBLE_COLUMN, "3" },
                { new TextNode("42"), INTEGER_COLUMN, null },
                { new IntNode(42), INTEGER_COLUMN, "42" },
                { new DecimalNode(new BigDecimal("3.14159")), INTEGER_COLUMN, "3" },
                { new IntNode(1024), STRING_COLUMN, "1024" },
                { new TextNode("foo"), STRING_COLUMN, "foo" },
                { new TextNode("aaaabbbbccccdddd"), STRING_COLUMN, "aaaabbbbcc" },
                { new TextNode("foo"), UNSUPPORTED_COLUMN, null },
        };
    }

    @Test(dataProvider = "serializeDataProvider")
    public void serialize(JsonNode node, ColumnSchema columnSchema, String expected) {
        String result = Utils.serializeJsonForColumn(node, columnSchema);
        assertEquals(result, expected);
    }

    @Test
    public void serializeLargeText() {
        ObjectNode node = DefaultObjectMapper.INSTANCE.createObjectNode();
        node.put("foo", "foo-value");
        node.put("bar", "bar-value");
        String result = Utils.serializeJsonForColumn(node, LARGETEXT_COLUMN);
        assertEquals(result, node.toString());
    }

    @Test
    public void writeRowToTsv() throws Exception {
        // Write
        String output;
        try (StringWriter stringWriter = new StringWriter();
                PrintWriter printWriter = new PrintWriter(stringWriter)) {
            Utils.writeRowToTsv(printWriter, ImmutableList.of("foo", "bar", "baz"));
            Utils.writeRowToTsv(printWriter, ImmutableList.of("qwerty", "asdf", "jkl;"));
            Utils.writeRowToTsv(printWriter, ImmutableList.of("AAA", "BBB", "CCC"));
            output = stringWriter.toString();
        }

        // Verify result
        String[] lines = output.split("\n");
        assertEquals(lines.length, 3);
        assertEquals(lines[0], "foo\tbar\tbaz");
        assertEquals(lines[1], "qwerty\tasdf\tjkl;");
        assertEquals(lines[2], "AAA\tBBB\tCCC");
    }
}
