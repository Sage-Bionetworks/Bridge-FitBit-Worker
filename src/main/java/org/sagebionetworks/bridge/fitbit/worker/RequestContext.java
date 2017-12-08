package org.sagebionetworks.bridge.fitbit.worker;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.sagebionetworks.bridge.rest.model.Study;

// todo doc
public class RequestContext {
    private final String date;
    private final Study study;
    private final File tmpDir;
    private final Map<String, PopulatedTable> populatedTablesById = new HashMap<>();

    public RequestContext(String date, Study study, File tmpDir) {
        this.date = date;
        this.study = study;
        this.tmpDir = tmpDir;
    }

    public String getDate() {
        return date;
    }

    public Study getStudy() {
        return study;
    }

    public File getTmpDir() {
        return tmpDir;
    }

    public Map<String, PopulatedTable> getPopulatedTablesById() {
        return populatedTablesById;
    }
}
