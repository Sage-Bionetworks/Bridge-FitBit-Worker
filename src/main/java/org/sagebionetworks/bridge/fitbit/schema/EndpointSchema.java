package org.sagebionetworks.bridge.fitbit.schema;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Maps;

// todo doc
public class EndpointSchema {
    private String endpointId;
    private Set<String> ignoredKeys;
    private String url;
    private List<UrlParameterType> urlParameters;
    private List<TableSchema> tables;
    private Map<String, TableSchema> tablesByKey;

    public String getEndpointId() {
        return endpointId;
    }

    public void setEndpointId(String endpointId) {
        this.endpointId = endpointId;
    }

    public Set<String> getIgnoredKeys() {
        return ignoredKeys;
    }

    public void setIgnoredKeys(Set<String> ignoredKeys) {
        this.ignoredKeys = ignoredKeys;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public List<UrlParameterType> getUrlParameters() {
        return urlParameters;
    }

    public void setUrlParameters(List<UrlParameterType> urlParameters) {
        this.urlParameters = urlParameters;
    }

    public List<TableSchema> getTables() {
        return tables;
    }

    @JsonIgnore
    public Map<String, TableSchema> getTablesByKey() {
        return tablesByKey;
    }

    public void setTables(List<TableSchema> tables) {
        this.tables = tables;
        this.tablesByKey = Maps.uniqueIndex(tables, TableSchema::getTableKey);
    }
}
