package org.egov.collection.service;

import org.apache.commons.lang3.StringUtils;
import org.egov.collection.config.ApplicationProperties;
import org.egov.collection.repository.PaymentRepository;
import org.egov.collection.repository.ServiceRequestRepository;
import org.egov.tracer.model.CustomException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class PreExistPaymentService {

	@Autowired
	private ApplicationProperties applicationProperties;

	@Autowired
	private PaymentRepository paymentRepository;

	@Autowired
	private ServiceRequestRepository serviceRequestRepository;

	@Autowired
	private ObjectMapper mapper;

	/**
	 * Fetch the Bank details from bank ifsccode
	 * 
	 * @param ifscCode
	 * @return
	 */
	private JsonNode populateBankBranch(String ifscCode) {
		JsonNode razorpayIfsccodeResponse = null;
		if (StringUtils.isNotEmpty(ifscCode)) {
			String response = serviceRequestRepository
					.fetchGetResult(applicationProperties.getRazorPayUrl() + ifscCode);
			try {
				razorpayIfsccodeResponse = mapper.readTree(response);
			} catch (JsonProcessingException e) {
				throw new CustomException("INVALID_PROCESS_EXCEPTION", e.getMessage());
			}
		}
		return razorpayIfsccodeResponse;
	}

	/**
	 * From the payment ifsccode get bank details from RazorPay api. Persists
	 * the bankdetails in payment additional details.
	 * 
	 * @param ifsccode
	 */
	@Transactional
	public void updatePaymentBankDetails(String ifsccode) {
		JsonNode bandetails = populateBankBranch(ifsccode);
		paymentRepository.updatePaymentBankDetail(bandetails, ifsccode);
	}

}
