package org.sagebionetworks.bridge.fitbit.bridge;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import retrofit2.Call;
import retrofit2.Response;

import org.sagebionetworks.bridge.fitbit.worker.Constants;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.ForwardCursorStringList;
import org.sagebionetworks.bridge.rest.model.OAuthAccessToken;

@SuppressWarnings("unchecked")
public class FitBitUserIteratorTest {
    private static final String ACCESS_TOKEN_PREFIX = "dummy-access-token-";
    private static final String HEALTH_CODE_PREFIX = "dummy-health-code-";
    private static final String STUDY_ID = "test-study";
    private static final String USER_ID_PREFIX = "dummy-user-id-";

    private ClientManager mockClientManager;
    private ForWorkersApi mockApi;

    @BeforeMethod
    public void setup() {
        mockApi = mock(ForWorkersApi.class);

        mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockApi);
    }

    @Test
    public void testWith0Users() throws Exception {
        // mockApiWithPage() with start=0 and end=-1 does what we want, even though it reads funny.
        mockApiWithPage(null, 0, -1, null);

        // Verify iterator has no users
        FitBitUserIterator iter = new FitBitUserIterator(mockClientManager, STUDY_ID);
        assertFalse(iter.hasNext());
    }

    @Test
    public void testWith1User() throws Exception {
        mockApiWithPage(null, 0, 0, null);
        testIterator(1);
    }

    @Test
    public void testWith1Page() throws Exception {
        mockApiWithPage(null, 0, 9, null);
        testIterator(10);
    }

    @Test
    public void testWith1PagePlus1User() throws Exception {
        mockApiWithPage(null, 0, 9, "page2");
        mockApiWithPage("page2", 10, 10, null);
        testIterator(11);
    }

    @Test
    public void testWith2Pages() throws Exception {
        mockApiWithPage(null, 0, 9, "page2");
        mockApiWithPage("page2", 10, 19, null);
        testIterator(20);
    }

    @Test
    public void hasNextDoesNotCallServerOrAdvanceIterator() throws Exception {
        // Create page with 2 items
        mockApiWithPage(null, 0, 1, null);

        // Create iterator. Verify initial call to server.
        FitBitUserIterator iter = new FitBitUserIterator(mockClientManager, STUDY_ID);
        verify(mockApi).getHealthCodesGrantingOAuthAccess(STUDY_ID, Constants.FITBIT_VENDOR_ID,
                FitBitUserIterator.PAGESIZE, null);

        // Make a few extra calls to hasNext(). Verify that no server calls are made
        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext());
        verifyNoMoreInteractions(mockApi);

        // next() still points to the first element
        FitBitUser firstUser = iter.next();
        assertFitBitUserForIndex(0, firstUser);
    }

    // branch coverage
    @Test(expectedExceptions = RuntimeException.class)
    public void errorGettingPage() throws Exception {
        // Mock page call to throw
        Call<ForwardCursorStringList> mockPageCall = mock(Call.class);
        when(mockPageCall.execute()).thenThrow(IOException.class);

        when(mockApi.getHealthCodesGrantingOAuthAccess(STUDY_ID, Constants.FITBIT_VENDOR_ID,
                FitBitUserIterator.PAGESIZE, null)).thenReturn(mockPageCall);

        // Execute
        new FitBitUserIterator(mockClientManager, STUDY_ID);
    }

    // branch coverage
    @Test(expectedExceptions = RuntimeException.class)
    public void errorGettingToken() throws Exception {
        // Mock page call to return one item
        ForwardCursorStringList forwardCursorStringList = new ForwardCursorStringList();
        forwardCursorStringList.addItemsItem(HEALTH_CODE_PREFIX);
        forwardCursorStringList.setHasNext(false);
        forwardCursorStringList.setNextPageOffsetKey(null);

        Response<ForwardCursorStringList> pageResponse = Response.success(forwardCursorStringList);

        Call<ForwardCursorStringList> mockPageCall = mock(Call.class);
        when(mockPageCall.execute()).thenReturn(pageResponse);

        when(mockApi.getHealthCodesGrantingOAuthAccess(STUDY_ID, Constants.FITBIT_VENDOR_ID,
                FitBitUserIterator.PAGESIZE, null)).thenReturn(mockPageCall);

        // Mock token call to throw
        Call<OAuthAccessToken> mockTokenCall = mock(Call.class);
        when(mockTokenCall.execute()).thenThrow(IOException.class);

        when(mockApi.getOAuthAccessToken(STUDY_ID, Constants.FITBIT_VENDOR_ID, HEALTH_CODE_PREFIX)).thenReturn(
                mockTokenCall);

        // Execute
        FitBitUserIterator iter = new FitBitUserIterator(mockClientManager, STUDY_ID);
        assertTrue(iter.hasNext());
        iter.next();
    }

    // branch coverage
    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "No more tokens left for study " + STUDY_ID)
    public void extraCallToNextThrows() throws Exception {
        // Mock page with just 1 item
        mockApiWithPage(null, 0, 0, null);

        // next() twice throws
        FitBitUserIterator iter = new FitBitUserIterator(mockClientManager, STUDY_ID);
        iter.next();
        iter.next();
    }

    private void mockApiWithPage(String curOffsetKey, int start, int end, String nextPageOffsetKey) throws Exception {
        ForwardCursorStringList forwardCursorStringList = new ForwardCursorStringList();

        // Make page elements
        List<String> healthCodeList = new ArrayList<>();
        for (int i = start; i <= end; i++) {
            healthCodeList.add(HEALTH_CODE_PREFIX + i);
        }
        forwardCursorStringList.setItems(healthCodeList);

        // hasNext and nextPageOffsetKey
        forwardCursorStringList.setHasNext(nextPageOffsetKey != null);
        forwardCursorStringList.setNextPageOffsetKey(nextPageOffsetKey);

        // Mock API to return this.
        Response<ForwardCursorStringList> pageResponse = Response.success(forwardCursorStringList);

        Call<ForwardCursorStringList> mockPageCall = mock(Call.class);
        when(mockPageCall.execute()).thenReturn(pageResponse);

        when(mockApi.getHealthCodesGrantingOAuthAccess(STUDY_ID, Constants.FITBIT_VENDOR_ID,
                FitBitUserIterator.PAGESIZE, curOffsetKey)).thenReturn(mockPageCall);

        // Mock token calls
        for (int i = start; i <= end; i++) {
            // Must mock access token because there are no setters.
            OAuthAccessToken token = mock(OAuthAccessToken.class);
            when(token.getAccessToken()).thenReturn(ACCESS_TOKEN_PREFIX + i);
            when(token.getProviderUserId()).thenReturn(USER_ID_PREFIX + i);

            // Mock API to return this
            Response<OAuthAccessToken> tokenResponse = Response.success(token);

            Call<OAuthAccessToken> mockTokenCall = mock(Call.class);
            when(mockTokenCall.execute()).thenReturn(tokenResponse);

            when(mockApi.getOAuthAccessToken(STUDY_ID, Constants.FITBIT_VENDOR_ID, HEALTH_CODE_PREFIX + i))
                    .thenReturn(mockTokenCall);
        }
    }

    private void testIterator(int expectedCount) {
        FitBitUserIterator iter = new FitBitUserIterator(mockClientManager, STUDY_ID);

        int numUsers = 0;
        while (iter.hasNext()) {
            FitBitUser oneUser = iter.next();
            assertFitBitUserForIndex(numUsers, oneUser);
            numUsers++;
        }

        assertEquals(numUsers, expectedCount);
    }

    private void assertFitBitUserForIndex(int idx, FitBitUser user) {
        assertEquals(user.getAccessToken(), ACCESS_TOKEN_PREFIX + idx);
        assertEquals(user.getHealthCode(), HEALTH_CODE_PREFIX + idx);
        assertEquals(user.getUserId(), USER_ID_PREFIX + idx);
    }
}
