package com.ingestpipeline.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CollectionRef {


    private String fieldName;
    private String argument;
    private String dataType;

    @JsonProperty(value="fieldName")
    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    @JsonProperty(value="argument")
    public String getArgument() {
        return argument;
    }

    public void setArgument(String argument) {
        this.argument = argument;
    }

    @JsonProperty(value="dataType")
    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }
}
