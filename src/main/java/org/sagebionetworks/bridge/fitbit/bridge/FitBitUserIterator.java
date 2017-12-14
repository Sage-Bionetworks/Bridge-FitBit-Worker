package org.sagebionetworks.bridge.fitbit.bridge;

import java.io.IOException;
import java.util.Iterator;

import org.sagebionetworks.bridge.fitbit.worker.Constants;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.ForwardCursorStringList;
import org.sagebionetworks.bridge.rest.model.OAuthAccessToken;

/** Helper class to abstract away Bridge's paginated API for OAuth tokens. */
public class FitBitUserIterator implements Iterator<FitBitUser> {
    // Package-scoped for unit tests
    static final int PAGESIZE = 10;

    // Instance invariants
    private final ClientManager bridgeClientManager;
    private final String studyId;

    // Instance state tracking
    private ForwardCursorStringList healthCodeList;
    private int nextIndex;

    /**
     * Constructs a FitBitUserIterator for the given Bridge client and study. This kicks off requests to load the first
     * page.
     */
    public FitBitUserIterator(ClientManager bridgeClientManager, String studyId) {
        this.bridgeClientManager = bridgeClientManager;
        this.studyId = studyId;

        // Load first page. Pass in null offsetKey to get the first page.
        loadNextPage(null);
    }

    // Helper method to load the next page of users, using the offsetKey to request the next page. Pass in null to get
    // the first page.
    private void loadNextPage(String offsetKey) {
        // Call server for the next page.
        try {
            healthCodeList = bridgeClientManager.getClient(ForWorkersApi.class).getHealthCodesGrantingOAuthAccess(
                    studyId, Constants.FITBIT_VENDOR_ID, PAGESIZE, offsetKey).execute().body();
        } catch (IOException ex) {
            // Iterator can't throw exceptions. Wrap in a RuntimeException.
            throw new RuntimeException(ex);
        }

        // Reset nextIndex.
        nextIndex = 0;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        return hasNextItemInPage() || hasNextPage();
    }

    // Helper method to determine if there are additional items in this page.
    private boolean hasNextItemInPage() {
        return nextIndex < healthCodeList.getItems().size();
    }

    // Helper method to determine if there is a next page.
    private boolean hasNextPage() {
        return healthCodeList.getHasNext();
    }

    /** {@inheritDoc} */
    @Override
    public FitBitUser next() {
        if (hasNextItemInPage()) {
            return getNextFitBitUser();
        } else if (hasNextPage()) {
            loadNextPage(healthCodeList.getNextPageOffsetKey());
            return getNextFitBitUser();
        } else {
            throw new IllegalStateException("No more tokens left for study " + studyId);
        }
    }

    // Helper method to get the next FitBitUser for the next healthCode in the list.
    private FitBitUser getNextFitBitUser() {
        // Get next token for healthCode from server.
        String healthCode = healthCodeList.getItems().get(nextIndex);
        OAuthAccessToken token;
        try {
            token = bridgeClientManager.getClient(ForWorkersApi.class).getOAuthAccessToken(studyId,
                    Constants.FITBIT_VENDOR_ID, healthCode).execute().body();
        } catch (IOException ex) {
            // Iterator can't throw exceptions. Wrap in a RuntimeException.
            throw new RuntimeException(ex);
        }

        // Increment the nextIndex counter.
        nextIndex++;

        // Construct and return the FitBitUser.
        return new FitBitUser.Builder().withAccessToken(token.getAccessToken()).withHealthCode(healthCode)
                .withUserId(token.getProviderUserId()).build();
    }
}
