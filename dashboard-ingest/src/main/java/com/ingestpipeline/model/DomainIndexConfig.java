package com.ingestpipeline.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class DomainIndexConfig {

    private String id;
    private String businessType;
    private String indexName;
    private String documentType;
    private String query;

    private List<CollectionRef> collectionRef = new ArrayList<>();

    @JsonProperty(value="id")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @JsonProperty(value="businessType")
    public String getBusinessType() {
        return businessType;
    }

    public void setBusinessType(String businessType) {
        this.businessType = businessType;
    }

    @JsonProperty(value="indexName")
    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    @JsonProperty(value="documentType")
    public String getDocumentType() {
        return documentType;
    }

    public void setDocumentType(String documentType) {
        this.documentType = documentType;
    }

    @JsonProperty(value="query")
    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    @JsonProperty(value="collectionRef")
    public List<CollectionRef> getCollectionRef() {
        return collectionRef;
    }

    public void setCollectionRef(List<CollectionRef> collectionRef) {
        this.collectionRef = collectionRef;
    }
}
