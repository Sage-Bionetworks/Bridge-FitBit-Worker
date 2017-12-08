package org.sagebionetworks.bridge.fitbit.bridge;

import java.io.IOException;
import java.util.Iterator;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.ForwardCursorStringList;
import org.sagebionetworks.bridge.rest.model.OAuthAccessToken;

public class FitBitUserIterator implements Iterable<FitBitUser>, Iterator<FitBitUser> {
    private static final int PAGESIZE = 10;
    private static final String VENDOR_ID = "fitbit";

    private final ClientManager bridgeClientManager;
    private final String studyId;

    private ForwardCursorStringList healthCodeList;
    private int nextIndex;

    public FitBitUserIterator(ClientManager bridgeClientManager, String studyId) {
        this.bridgeClientManager = bridgeClientManager;
        this.studyId = studyId;

        loadNextPage();
    }

    private void loadNextPage() {
        // If it's the first page (no healthCodeList yet), the offsetKey is null;
        String offsetKey = healthCodeList != null ? healthCodeList.getNextPageOffsetKey() : null;

        // Call server for the next page.
        try {
            healthCodeList = bridgeClientManager.getClient(ForWorkersApi.class).getHealthCodesGrantingOAuthAccess(
                    studyId, VENDOR_ID, PAGESIZE, offsetKey).execute().body();
        } catch (IOException ex) {
            // Iterator can't throw exceptions. Wrap in a RuntimeException.
            throw new RuntimeException(ex);
        }

        // Reset nextIndex.
        nextIndex = 0;
    }

    @Override
    public Iterator<FitBitUser> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return hasNextItemInPage() || hasNextPage();
    }

    private boolean hasNextItemInPage() {
        return nextIndex < healthCodeList.getItems().size();
    }

    private boolean hasNextPage() {
        return healthCodeList.getHasNext();
    }

    @Override
    public FitBitUser next() {
        if (hasNextItemInPage()) {
            return getNextFitBitUser();
        } else if (hasNextPage()) {
            loadNextPage();
            return getNextFitBitUser();
        } else {
            throw new IllegalStateException("No more tokens left for study " + studyId);
        }
    }

    private FitBitUser getNextFitBitUser() {
        // Get next token for healthCode from server.
        String healthCode = healthCodeList.getItems().get(nextIndex);
        OAuthAccessToken token;
        try {
            token = bridgeClientManager.getClient(ForWorkersApi.class).getOAuthAccessToken(studyId, VENDOR_ID,
                    healthCode).execute().body();
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
