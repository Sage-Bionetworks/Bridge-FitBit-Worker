package org.sagebionetworks.bridge.fitbit.worker;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.exceptions.BridgeSynapseException;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.fitbit.schema.ColumnSchema;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.synapse.SynapseHelper;

@Component
public class TableProcessor {
    private static final Joiner JOINER_COLUMN_JOINER = Joiner.on('\t').useForNull("");

    private static final List<ColumnSchema> COMMON_COLUMN_LIST = ImmutableList.of(
            new ColumnSchema.Builder().withColumnId(Constants.COLUMN_HEALTH_CODE).withColumnType(ColumnType.STRING)
                    .withMaxLength(36).build(),
            new ColumnSchema.Builder().withColumnId(Constants.COLUMN_CREATED_DATE).withColumnType(ColumnType.STRING)
                    .withMaxLength(10).build());

    private Table ddbTablesMap;
    private FileHelper fileHelper;
    private SynapseHelper synapseHelper;
    private long synapsePrincipalId;

    @Resource(name = "ddbTablesMap")
    public final void setDdbTablesMap(Table ddbTablesMap) {
        this.ddbTablesMap = ddbTablesMap;
    }

    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    @Autowired
    public final void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    @Resource(name = "synapsePrincipalId")
    public final void setSynapsePrincipalId(long synapsePrincipalId) {
        this.synapsePrincipalId = synapsePrincipalId;
    }

    // todo refactor
    public void processTable(RequestContext ctx, PopulatedTable table) throws BridgeSynapseException,
            IOException, SynapseException {
        if (table.getRowList().isEmpty()) {
            // No data. Skip.
            return;
        }

        convertInMemoryTableToTsv(ctx, table);
        String synapseTableId = verifySynapseTable(ctx, table);

        File tsvFile = table.getTsvFile();
        long linesProcessed = synapseHelper.uploadTsvFileToTable(synapseTableId, tsvFile);
        int expectedLineCount = table.getRowList().size();
        if (linesProcessed != expectedLineCount) {
            throw new BridgeSynapseException("Wrong number of lines processed importing to table=" + synapseTableId +
                    ", expected=" + expectedLineCount + ", actual=" + linesProcessed);
        }

        // We've successfully processed the file. We can delete the file now.
        fileHelper.deleteFile(tsvFile);
    }

    // todo refactor
    public void convertInMemoryTableToTsv(RequestContext ctx, PopulatedTable table) throws FileNotFoundException {
        // Combine common columns with table-specific columns.
        List<ColumnSchema> allColumnList = getAllColumnsForTable(table);
        List<String> allColumnNameList = allColumnList.stream().map(ColumnSchema::getColumnId).collect(Collectors
                .toList());

        // Set up file writer
        File tsvFile = fileHelper.newFile(ctx.getTmpDir(), table.getTableId() + ".tsv");
        table.setTsvFile(tsvFile);
        try (PrintWriter tsvWriter = new PrintWriter(fileHelper.getWriter(tsvFile))) {
            // Write headers. (Headers also include healthCode and createdDate.)
            writeRow(tsvWriter, allColumnNameList);

            // Each table row is a map. Go in order of columns and write TSV rows.
            for (Map<String, String> oneRowValueMap : table.getRowList()) {
                List<String> oneRowValueList = allColumnNameList.stream().map(oneRowValueMap::get).collect(Collectors
                        .toList());
                writeRow(tsvWriter, oneRowValueList);
            }
        }
    }

    // todo refactor
    public List<ColumnSchema> getAllColumnsForTable(PopulatedTable table) {
        // Combine common columns with table-specific columns.
        List<ColumnSchema> allColumnList = new ArrayList<>();
        allColumnList.addAll(COMMON_COLUMN_LIST);
        allColumnList.addAll(table.getTableSchema().getColumns());
        return allColumnList;
    }

    // todo refactor
    public void writeRow(PrintWriter tsvWriter, List<String> rowValueList) {
        tsvWriter.println(JOINER_COLUMN_JOINER.join(rowValueList));
    }

    public String verifySynapseTable(RequestContext ctx, PopulatedTable table) throws BridgeSynapseException,
            SynapseException {
        // todo: consolidate this with similar logic in Bridge EX
        String tableId = table.getTableId();

        // Check if we have this in DDB
        String synapseTableId = getSynapseTableIdFromDdb(ctx.getStudy().getIdentifier(), tableId);

        // Check if the table exists in Synapse
        boolean tableExists = synapseTableId != null;
        if (tableExists) {
            try {
                synapseHelper.getTableWithRetry(synapseTableId);
            } catch (SynapseNotFoundException e) {
                tableExists = false;
            }
        }

        // Convert ColumnSchemas to Synapse Column Models.
        // Combine common columns with table-specific columns.
        List<ColumnSchema> allColumnList = getAllColumnsForTable(table);
        List<ColumnModel> columnModelList = allColumnList.stream().map(this::getColumnModelForSchema).collect(
                Collectors.toList());

        if (!tableExists) {
            // Delegate table creation to SynapseHelper.
            // todo null checks
            Study study = ctx.getStudy();
            long dataAccessTeamId = study.getSynapseDataAccessTeamId();
            String projectId = study.getSynapseProjectId();
            String newSynapseTableId = synapseHelper.createTableWithColumnsAndAcls(columnModelList, dataAccessTeamId,
                    synapsePrincipalId, projectId, tableId);

            // write back to DDB table
            setSynapseTableIdToDdb(study.getIdentifier(), tableId, newSynapseTableId);
            return newSynapseTableId;
        } else {
            synapseHelper.safeUpdateTable(synapseTableId, columnModelList);
            return synapseTableId;
        }
    }

    public String getSynapseTableIdFromDdb(String studyId, String tableId) {
        Item tableMapItem = ddbTablesMap.getItem("studyId", studyId, "tableId", tableId);
        if (tableMapItem != null) {
            return tableMapItem.getString("synapseTableId");
        } else {
            return null;
        }
    }

    public void setSynapseTableIdToDdb(String studyId, String tableId, String synapseTableId) {
        Item tableMapItem = new Item().withString("studyId", studyId).withString("tableId", tableId)
                .withString("synapseTableId", synapseTableId);
        ddbTablesMap.putItem(tableMapItem);
    }

    public ColumnModel getColumnModelForSchema(ColumnSchema schema) {
        // Column ID in the schema is column name in the model.
        ColumnModel columnModel = new ColumnModel();
        columnModel.setName(schema.getColumnId());
        columnModel.setColumnType(schema.getColumnType());
        if (schema.getMaxLength() != null) {
            columnModel.setMaximumSize(schema.getMaxLength().longValue());
        }
        return columnModel;
    }
}
