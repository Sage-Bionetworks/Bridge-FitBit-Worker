package org.sagebionetworks.bridge.fitbit.bridge;

import java.io.IOException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.model.Study;

@Component
public class BridgeHelper {
    private ClientManager clientManager;

    @Autowired
    public final void setClientManager(ClientManager clientManager) {
        this.clientManager = clientManager;
    }

    public FitBitUserIterator getFitBitUsersForStudy(String studyId) {
        return new FitBitUserIterator(clientManager, studyId);
    }

    public List<Study> getAllStudies() throws IOException {
        return clientManager.getClient(StudiesApi.class).getStudies(/* summary */true).execute().body()
                .getItems();
    }
}
