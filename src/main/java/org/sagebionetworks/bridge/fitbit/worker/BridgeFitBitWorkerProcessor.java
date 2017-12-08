package org.sagebionetworks.bridge.fitbit.worker;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.exceptions.BridgeSynapseException;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.fitbit.bridge.BridgeHelper;
import org.sagebionetworks.bridge.fitbit.bridge.FitBitUserIterator;
import org.sagebionetworks.bridge.fitbit.bridge.FitBitUser;
import org.sagebionetworks.bridge.fitbit.schema.EndpointSchema;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;

// todo doc
@Component("FitBitWorker")
public class BridgeFitBitWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeFitBitWorkerProcessor.class);

    private static final int REPORTING_INTERVAL = 10;

    private final RateLimiter perUserRateLimiter = RateLimiter.create(1.0);

    private BridgeHelper bridgeHelper;
    private List<EndpointSchema> endpointSchemas;
    private FileHelper fileHelper;
    private TableProcessor tableProcessor;
    private UserProcessor userProcessor;

    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    @Resource(name = "endpointSchemas")
    public final void setEndpointSchemas(List<EndpointSchema> endpointSchemas) {
        this.endpointSchemas = endpointSchemas;
    }

    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    public final void setPerUserRateLimit(double rate) {
        perUserRateLimiter.setRate(rate);
    }

    @Autowired
    public final void setTableProcessor(TableProcessor tableProcessor) {
        this.tableProcessor = tableProcessor;
    }

    @Autowired
    public final void setUserProcessor(UserProcessor userProcessor) {
        this.userProcessor = userProcessor;
    }

    // todo doc
    @Override
    public void accept(JsonNode jsonNode) throws BridgeSynapseException, IOException, SynapseException {
        // todo error handling
        // Get request args.
        String dateString = jsonNode.get("date").textValue();

        LOG.info("Received request for date " + dateString);
        Stopwatch requestStopwatch = Stopwatch.createStarted();
        try {
            List<Study> studyList = bridgeHelper.getAllStudies();
            for (Study oneStudy : studyList) {
                String studyId = oneStudy.getIdentifier();

                if (oneStudy.getSynapseProjectId() == null || oneStudy.getSynapseDataAccessTeamId() == null ||
                        oneStudy.getOAuthProviders() == null || !oneStudy.getOAuthProviders().containsKey("fitbit")) {
                    // This study is either not configured for Synapse or is not configured for FitBit. Skip.
                    LOG.info("Skipping study " + studyId);
                }

                LOG.info("Processing study " + studyId);
                Stopwatch studyStopwatch = Stopwatch.createStarted();
                try {
                    // Set up request context
                    File tmpDir = fileHelper.createTempDir();
                    RequestContext ctx = new RequestContext(dateString, oneStudy, tmpDir);

                    // Get list of users (and their keys)
                    FitBitUserIterator fitBitUserIter = bridgeHelper.getFitBitUsersForStudy(oneStudy.getIdentifier());
                    LOG.info("Processing users in study " + studyId);
                    int numUsers = 0;
                    Stopwatch userStopwatch = Stopwatch.createStarted();
                    try {
                        for (FitBitUser oneUser : fitBitUserIter) {
                            perUserRateLimiter.acquire();

                            // Call and process endpoints.
                            for (EndpointSchema oneEndpointSchema : endpointSchemas) {
                                userProcessor.processEndpointForUser(ctx, oneUser, oneEndpointSchema);
                            }

                            // Reporting
                            numUsers++;
                            if (numUsers % REPORTING_INTERVAL == 0) {
                                LOG.info("Processing users in progress: " + numUsers + " users in " +
                                        userStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
                            }
                        }
                    } finally {
                        LOG.info("Finished processing users: " + numUsers + " users in " +
                                userStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
                    }

                    // Process and upload each table
                    for (PopulatedTable onePopulatedTable : ctx.getPopulatedTablesById().values()) {
                        String tableId = onePopulatedTable.getTableId();
                        LOG.info("Processing table " + tableId);
                        Stopwatch tableStopwatch = Stopwatch.createStarted();
                        try {
                            tableProcessor.processTable(ctx, onePopulatedTable);
                        } finally {
                            LOG.info("Finished processing table " + tableId + " in " +
                                    tableStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
                        }
                    }
                } finally {
                    LOG.info("Finished processing study " + studyId + " in " +
                            studyStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
                }
            }
        } finally {
            LOG.info("Finished processing request for date " + dateString + " in " +
                    requestStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
        }
    }
}
