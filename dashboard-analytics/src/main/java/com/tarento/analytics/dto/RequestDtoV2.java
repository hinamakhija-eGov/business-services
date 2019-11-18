package com.tarento.analytics.dto;

import java.util.Map;

public class RequestDtoV2 {

	private Map<String, Object> headers;
	private AggregateRequestDtoV2 aggregationRequestDto;
	
	public Map<String, Object> getHeaders() {
		return headers;
	}
	public void setHeaders(Map<String, Object> headers) {
		this.headers = headers;
	}
	public AggregateRequestDtoV2 getAggregationRequestDto() {
		return aggregationRequestDto;
	}
	public void setAggregationRequestDto(AggregateRequestDtoV2 aggregationRequestDto) {
		this.aggregationRequestDto = aggregationRequestDto;
	} 
}
