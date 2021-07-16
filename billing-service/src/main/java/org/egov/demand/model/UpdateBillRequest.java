package org.egov.demand.model;

import org.egov.common.contract.request.RequestInfo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UpdateBillRequest {

	private RequestInfo RequestInfo;
	
	private UpdateBillCriteria UpdateBillCriteria;
}
