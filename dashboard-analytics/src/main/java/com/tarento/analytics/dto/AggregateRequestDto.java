package com.tarento.analytics.dto;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tarento.analytics.enums.ChartType;

public class AggregateRequestDto {

	@JsonProperty("chartType")
	private ChartType chartType;
	
	@JsonProperty("chartFormat")
	private String chartFormat;
	
	@JsonProperty("serviceApi")
	private String serviceApi;
	
	@JsonProperty("customData")
	private Map<String, Object> customData;
	
	@JsonProperty("dates")
	private RequestDate dates;
	
	@JsonProperty("interval")
	private String interval;

	public ChartType getChartType() {
		return chartType;
	}

	public void setChartType(ChartType chartType) {
		this.chartType = chartType;
	}

	public String getChartFormat() {
		return chartFormat;
	}

	public void setChartFormat(String chartFormat) {
		this.chartFormat = chartFormat;
	}

	public String getServiceApi() {
		return serviceApi;
	}

	public void setServiceApi(String serviceApi) {
		this.serviceApi = serviceApi;
	}

	public Map<String, Object> getCustomData() {
		return customData;
	}

	public void setCustomData(Map<String, Object> customData) {
		this.customData = customData;
	}

	public RequestDate getDates() {
		return dates;
	}

	public void setDates(RequestDate dates) {
		this.dates = dates;
	}

	public String getInterval() {
		return interval;
	}

	public void setInterval(String interval) {
		this.interval = interval;
	}
}


