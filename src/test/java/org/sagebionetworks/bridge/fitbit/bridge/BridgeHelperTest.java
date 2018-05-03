package org.sagebionetworks.bridge.fitbit.bridge;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import retrofit2.Call;
import retrofit2.Response;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.model.ForwardCursorStringList;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyList;

@SuppressWarnings("unchecked")
public class BridgeHelperTest {
    private static final String STUDY_ID = "test-study";

    private ClientManager mockClientManager;
    private BridgeHelper bridgeHelper;

    @BeforeMethod
    public void setup() {
        mockClientManager = mock(ClientManager.class);

        bridgeHelper = new BridgeHelper();
        bridgeHelper.setClientManager(mockClientManager);
    }

    @Test
    public void getFitBitUsersForStudy() throws Exception {
        // Mock client manager call to getHealthCodesGrantingOAuthAccess(). We don't care about the result. This is
        // tested in FitBitUserIterator.
        ForWorkersApi mockApi = mock(ForWorkersApi.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockApi);

        Call<ForwardCursorStringList> mockCall = mockCallForValue(null);
        when(mockApi.getHealthCodesGrantingOAuthAccess(any(), any(), any(), any())).thenReturn(mockCall);

        // Execute
        Iterator<FitBitUser> fitBitUserIter = bridgeHelper.getFitBitUsersForStudy(STUDY_ID);

        // Verify basics, like return value is not null, and we called the API with the right study ID.
        assertNotNull(fitBitUserIter);
        verify(mockApi).getHealthCodesGrantingOAuthAccess(eq(STUDY_ID), any(), any(), any());
    }

    @Test
    public void getAllStudies() throws Exception {
        // Mock client manager call to getAllStudies(). Note that study summaries only include study ID.
        StudiesApi mockApi = mock(StudiesApi.class);
        when(mockClientManager.getClient(StudiesApi.class)).thenReturn(mockApi);

        List<Study> studyListCol = ImmutableList.of(new Study().identifier("foo-study"), new Study().identifier(
                "bar-study"));
        StudyList studyListObj = new StudyList().items(studyListCol);
        Call<StudyList> mockCall = mockCallForValue(studyListObj);
        when(mockApi.getStudies(true)).thenReturn(mockCall);

        // Execute and validate
        List<Study> retVal = bridgeHelper.getAllStudies();
        assertEquals(retVal, studyListCol);
    }

    @Test
    public void getStudy() throws Exception {
        // Mock client manager call to getStudy. This contains dummy values for Synapse Project ID and Team ID to
        // "test" that our Study object is complete.
        ForWorkersApi mockApi = mock(ForWorkersApi.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockApi);

        Study study = new Study().identifier("my-study").synapseProjectId("my-project").synapseDataAccessTeamId(1111L);
        Call<Study> mockCall = mockCallForValue(study);
        when(mockApi.getStudy("my-study")).thenReturn(mockCall);

        // Execute and validate
        Study retVal = bridgeHelper.getStudy("my-study");
        assertEquals(retVal, study);
    }

    private static <T> Call<T> mockCallForValue(T value) throws Exception {
        Response<T> response = Response.success(value);

        Call<T> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        return mockCall;
    }
}
