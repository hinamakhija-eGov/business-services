package com.tarento.analytics.dto;

import java.util.Map;

public class AggregateRequestDtoV2 {
	
	private String visualizationType; 
	private String visualizationCode;
	private String moduleLevel; 
	private String queryType;
	private Map<String, Object> filters; 
	private Map<String, Object> esFilters; 
	private Map<String, Object> aggregationFactors; 
	private RequestDate requestDate; 
	private String interval;
	
	public String getModuleLevel() {
		return moduleLevel;
	}
	public void setModuleLevel(String moduleLevel) {
		this.moduleLevel = moduleLevel;
	}
	public Map<String, Object> getEsFilters() {
		return esFilters;
	}
	public void setEsFilters(Map<String, Object> esFilters) {
		this.esFilters = esFilters;
	}
	public String getVisualizationCode() {
		return visualizationCode;
	}
	public void setVisualizationCode(String visualizationCode) {
		this.visualizationCode = visualizationCode;
	}
	public String getVisualizationType() {
		return visualizationType;
	}
	public void setVisualizationType(String visualizationType) {
		this.visualizationType = visualizationType;
	}
	public String getQueryType() {
		return queryType;
	}
	public void setQueryType(String queryType) {
		this.queryType = queryType;
	}
	public Map<String, Object> getFilters() {
		return filters;
	}
	public void setFilters(Map<String, Object> filters) {
		this.filters = filters;
	}
	public Map<String, Object> getAggregationFactors() {
		return aggregationFactors;
	}
	public void setAggregationFactors(Map<String, Object> aggregationFactors) {
		this.aggregationFactors = aggregationFactors;
	}
	public RequestDate getRequestDate() {
		return requestDate;
	}
	public void setRequestDate(RequestDate requestDate) {
		this.requestDate = requestDate;
	}
	public String getInterval() {
		return interval;
	}
	public void setInterval(String interval) {
		this.interval = interval;
	} 
	
	

}
