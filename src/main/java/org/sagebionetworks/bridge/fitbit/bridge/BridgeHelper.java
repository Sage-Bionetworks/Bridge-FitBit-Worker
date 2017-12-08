package org.sagebionetworks.bridge.fitbit.bridge;

import java.io.IOException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.model.Study;

/** Encapsulates calls to Bridge server. */
@Component
public class BridgeHelper {
    private ClientManager clientManager;

    /** Bridge client manager. */
    @Autowired
    public final void setClientManager(ClientManager clientManager) {
        this.clientManager = clientManager;
    }

    /** Gets an iterator for all FitBit users in the given study. */
    public FitBitUserIterator getFitBitUsersForStudy(String studyId) {
        return new FitBitUserIterator(clientManager, studyId);
    }

    /** Gets all study summaries (worker API, active studies only). */
    public List<Study> getAllStudies() throws IOException {
        return clientManager.getClient(StudiesApi.class).getStudies(/* summary */true).execute().body()
                .getItems();
    }
}
