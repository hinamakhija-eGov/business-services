package org.egov.demand.web.controller;

import javax.validation.Valid;

import org.egov.common.contract.request.RequestInfo;
import org.egov.demand.helper.BillHelperV2;
import org.egov.demand.model.BillSearchCriteria;
import org.egov.demand.model.GenerateBillCriteria;
import org.egov.demand.service.BillServicev2;
import org.egov.demand.web.contract.BillRequestV2;
import org.egov.demand.web.contract.BillResponseV2;
import org.egov.demand.web.contract.CancelBillCriteria;
import org.egov.demand.web.contract.RequestInfoWrapper;
import org.egov.demand.web.validator.BillValidator;
import org.egov.tracer.model.CustomException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import org.egov.demand.util.Constants;

@RestController
@RequestMapping("bill/v2/")
public class BillControllerv2 {
	
	@Autowired
	private BillServicev2 billService;
	
	@Autowired
	private BillValidator billValidator;
	
	@Autowired
	private BillHelperV2 billHelper;
	
	@PostMapping("_search")
	@ResponseBody
	public ResponseEntity<?> search(@RequestBody @Valid final RequestInfoWrapper requestInfoWrapper,
			@ModelAttribute @Valid final BillSearchCriteria billCriteria) {

		RequestInfo requestInfo = requestInfoWrapper.getRequestInfo();
		billValidator.validateBillSearchCriteria(billCriteria, requestInfo);
		return new ResponseEntity<>(billService.searchBill(billCriteria,requestInfo), HttpStatus.OK);
	}


	@PostMapping("_fetchbill")
	@ResponseBody
	public ResponseEntity<?> fetchBill(@RequestBody RequestInfoWrapper requestInfoWrapper, 
			@ModelAttribute @Valid GenerateBillCriteria generateBillCriteria){
		
		String ojb = new JSONObject(generateBillCriteria).toString();
		System.out.println(" fetchBill GenerateBillCriteria::"+ ojb);
		
		BillResponseV2 billResponse = billService.fetchBill(generateBillCriteria, requestInfoWrapper);
		return new ResponseEntity<>(billResponse, HttpStatus.CREATED);
	}
	
	
	@PostMapping("_generate")
	@ResponseBody
	public ResponseEntity<?> genrateBill(@RequestBody RequestInfoWrapper requestInfoWrapper,
			@ModelAttribute @Valid GenerateBillCriteria generateBillCriteria) {

		throw new CustomException("EG_BS_API_ERROR", "The Generate bill API has been deprecated, Access the fetchBill");
	}
	
	@PostMapping("_create")
	@ResponseBody
	public ResponseEntity<?> create(@RequestBody BillRequestV2 billRequest, BindingResult bindingResult){

		BillResponseV2 billResponse = billService.sendBillToKafka(billRequest);
		billHelper.getBillRequestWithIds(billRequest);
		return new ResponseEntity<>(billResponse,HttpStatus.CREATED);
	}
	
	@PostMapping("_cancelbill")
	@ResponseBody
	public ResponseEntity<?> cancelBill(@RequestBody RequestInfoWrapper requestInfoWrapper, 
			@ModelAttribute @Valid CancelBillCriteria cancelBillCriteria){
		
		billService.cancelBill(cancelBillCriteria);
		return new ResponseEntity<>(Constants.SUCCESS_CANCEL_BILL, HttpStatus.CREATED);
	}
}
