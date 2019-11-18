package com.tarento.analytics.dto;

import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Data {

    private String headerName;
    private Object headerValue;
    private String headerSymbol;

    public Data(String name, Object value, String symbol) {
        this.headerName = name;
        this.headerValue = value;
        this.headerSymbol = symbol;
    }

    public Data(String name, Object value, String symbol, List<Plot> plots) {
        this.headerName = name;
        this.headerValue = value;
        this.headerSymbol = symbol;
        this.plots = plots;
    }

    private List<Plot> plots = new ArrayList<>();

    public List<Plot> getPlots() {
        return plots;
    }

    public void setPlots(List<Plot> plots) {
        this.plots = plots;
    }

    public void setHeaderValue(Object headerValue) {
        this.headerValue = headerValue;
    }

    public String getHeaderName() {
        return headerName;
    }

    public Object getHeaderValue() {
        return headerValue;
    }

    public String getHeaderSymbol() {
        return headerSymbol;
    }



}
