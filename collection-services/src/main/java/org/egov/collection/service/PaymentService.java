package org.egov.collection.service;

import static java.util.Objects.isNull;
import lombok.extern.slf4j.Slf4j;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.egov.collection.config.ApplicationProperties;
import org.egov.collection.model.Payment;
import org.egov.collection.model.PaymentRequest;
import org.egov.collection.model.PaymentSearchCriteria;
import org.egov.collection.producer.CollectionProducer;
import org.egov.collection.repository.PaymentRepository;
import org.egov.collection.util.PaymentEnricher;
import org.egov.collection.util.PaymentValidator;
import org.egov.collection.web.contract.Bill;
import org.egov.common.contract.request.RequestInfo;
import org.egov.tracer.model.CustomException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
@Service
public class PaymentService {

    private ApportionerService apportionerService;

    private PaymentEnricher paymentEnricher;

    private ApplicationProperties applicationProperties;

    private UserService userService;

    private PaymentValidator paymentValidator;

    private PaymentRepository paymentRepository;

    private CollectionProducer producer;
    
    @Autowired
	private ObjectMapper objectMapper;


    @Autowired
    public PaymentService(ApportionerService apportionerService, PaymentEnricher paymentEnricher, ApplicationProperties applicationProperties,
                          UserService userService, PaymentValidator paymentValidator, PaymentRepository paymentRepository, CollectionProducer producer) {
        this.apportionerService = apportionerService;
        this.paymentEnricher = paymentEnricher;
        this.applicationProperties = applicationProperties;
        this.userService = userService;
        this.paymentValidator = paymentValidator;
        this.paymentRepository = paymentRepository;
        this.producer = producer;
    }



    /**
     * Fetch all receipts matching the given criteria, enrich receipts with instruments
     *
     * @param requestInfo           Request info of the search
     * @param paymentSearchCriteria Criteria against which search has to be performed
     * @return List of matching receipts
     */
    public List<Payment> getPayments(RequestInfo requestInfo, PaymentSearchCriteria paymentSearchCriteria, String moduleName) {
    	
        paymentValidator.validateAndUpdateSearchRequestFromConfig(paymentSearchCriteria, requestInfo, moduleName);
        if (applicationProperties.isPaymentsSearchPaginationEnabled()) {
            paymentSearchCriteria.setOffset(isNull(paymentSearchCriteria.getOffset()) ? 0 : paymentSearchCriteria.getOffset());
            paymentSearchCriteria.setLimit(isNull(paymentSearchCriteria.getLimit()) ? applicationProperties.getReceiptsSearchDefaultLimit() :
                    paymentSearchCriteria.getLimit());
        } else {
            paymentSearchCriteria.setOffset(0);
            paymentSearchCriteria.setLimit(applicationProperties.getReceiptsSearchDefaultLimit());
        }
        /*if(requestInfo.getUserInfo().getType().equals("CITIZEN")) {
            List<String> payerIds = new ArrayList<>();
            payerIds.add(requestInfo.getUserInfo().getUuid());
            paymentSearchCriteria.setPayerIds(payerIds);
        }*/
        List<Payment> payments = paymentRepository.fetchPayments(paymentSearchCriteria);

        return payments;
    }


    
    
    /**
     * Handles creation of a receipt, including multi-service, involves the following steps, - Enrich receipt from billing service
     * using bill id - Validate the receipt object - Enrich receipt with receipt numbers, coll type etc - Apportion paid amount -
     * Persist the receipt object - Create instrument
     *
     * @param paymentRequest payment request for which receipt has to be created
     * @return Created receipt
     */
    @Transactional
    public Payment createPayment(PaymentRequest paymentRequest) {
    	
        paymentEnricher.enrichPaymentPreValidate(paymentRequest,false);
        paymentValidator.validatePaymentForCreate(paymentRequest);
        paymentEnricher.enrichPaymentPostValidate(paymentRequest);

        Payment payment = paymentRequest.getPayment();
        Map<String, Bill> billIdToApportionedBill = apportionerService.apportionBill(paymentRequest);
        paymentEnricher.enrichAdvanceTaxHead(new LinkedList<>(billIdToApportionedBill.values()));
        setApportionedBillsToPayment(billIdToApportionedBill,payment);

        String payerId = createUser(paymentRequest);
        if(!StringUtils.isEmpty(payerId))
            payment.setPayerId(payerId);
        paymentRepository.savePayment(payment);

        producer.producer(applicationProperties.getCreatePaymentTopicName(), paymentRequest);


        return payment;
    }


    /**
     * If Citizen is paying, the id of the logged in user becomes payer id.
     * If Employee is paying, 
     * 1. the id of the owner of the bill will be attached as payer id.
     * 2. In case the bill is for a misc payment, payer id is empty.
     * 
     * @param paymentRequest
     * @return
     */
    public String createUser(PaymentRequest paymentRequest) {
    	
        String id = null;
        if(paymentRequest.getRequestInfo().getUserInfo().getType().equals("CITIZEN")) {
            id = paymentRequest.getRequestInfo().getUserInfo().getUuid();
        }else {
            if(applicationProperties.getIsUserCreateEnabled()) {
                Payment payment = paymentRequest.getPayment();
                Map<String, String> res = userService.getUser(paymentRequest.getRequestInfo(), payment.getMobileNumber(), payment.getTenantId());
                if(CollectionUtils.isEmpty(res.keySet())) {
                    id = userService.createUser(paymentRequest);
                }else {
                    id = res.get("id");
                }
            }
        }
        return id;
    }


    private void setApportionedBillsToPayment(Map<String, Bill> billIdToApportionedBill,Payment payment){
        Map<String,String> errorMap = new HashMap<>();
        payment.getPaymentDetails().forEach(paymentDetail -> {
            if(billIdToApportionedBill.get(paymentDetail.getBillId())!=null)
                paymentDetail.setBill(billIdToApportionedBill.get(paymentDetail.getBillId()));
            else errorMap.put("APPORTIONING_ERROR","The bill id: "+paymentDetail.getBillId()+" not present in apportion response");
        });
        if(!errorMap.isEmpty())
            throw new CustomException(errorMap);
    }


    @Transactional
    public List<Payment> updatePayment(PaymentRequest paymentRequest) {

        List<Payment> validatedPayments = paymentValidator.validateAndEnrichPaymentsForUpdate(Collections.singletonList(paymentRequest.getPayment()),
                paymentRequest.getRequestInfo());

        paymentRepository.updatePayment(validatedPayments);
        producer.producer(applicationProperties.getUpdatePaymentTopicName(), new PaymentRequest(paymentRequest.getRequestInfo(), paymentRequest.getPayment()));

        return validatedPayments;
    }
    
    
    
    /**
     * Used by payment gateway to validate provisional receipts of the payment
     * 
     * @param paymentRequest
     * @return
     */
    @Transactional
    public Payment vaidateProvisonalPayment(PaymentRequest paymentRequest) {
        paymentEnricher.enrichPaymentPreValidate(paymentRequest,false);
        paymentValidator.validatePaymentForCreate(paymentRequest);
        
        return paymentRequest.getPayment();
    }
    
    @Transactional
    public Payment updatePaymentForFilestore(Payment payment) {

       paymentRepository.updateFileStoreIdToNull(payment);
        return payment;
    }
    
    public List<Payment> plainSearch(PaymentSearchCriteria paymentSearchCriteria) {
        PaymentSearchCriteria searchCriteria = new PaymentSearchCriteria();
        
	    log.info("plainSearch Service BusinessServices"+paymentSearchCriteria.getBusinessServices() +"plainSearch Service Date "+
        		 paymentSearchCriteria.getFromDate() +" to "+paymentSearchCriteria.getToDate() +"Teant IT "+paymentSearchCriteria.getTenantId()+" \"plainSearch Service BusinessServices\"+paymentSearchCriteria.getBusinessService():"+paymentSearchCriteria.getBusinessService());    
	    
        if (applicationProperties.isPaymentsSearchPaginationEnabled()) {
            searchCriteria.setOffset(isNull(paymentSearchCriteria.getOffset()) ? 0 : paymentSearchCriteria.getOffset());
            searchCriteria.setLimit(isNull(paymentSearchCriteria.getLimit()) ? applicationProperties.getReceiptsSearchDefaultLimit() : paymentSearchCriteria.getLimit());
        } else {
            searchCriteria.setOffset(0);
            searchCriteria.setLimit(applicationProperties.getReceiptsSearchDefaultLimit());
        }
        
        if(paymentSearchCriteria.getTenantId() != null) {
        	searchCriteria.setTenantId(paymentSearchCriteria.getTenantId());
        }
        
        if(paymentSearchCriteria.getBusinessServices() != null) {
		 log.info("in PaymentService.java paymentSearchCriteria.getBusinessServices(): " + paymentSearchCriteria.getBusinessServices());
        	searchCriteria.setBusinessServices(paymentSearchCriteria.getBusinessServices());
        }
        
        if(paymentSearchCriteria.getBusinessService() != null) {
   		 log.info("in PaymentService.java paymentSearchCriteria.getBusinessService(): " + paymentSearchCriteria.getBusinessService());
           	searchCriteria.setBusinessService(paymentSearchCriteria.getBusinessService());
          }
           

        
        
        if((paymentSearchCriteria.getFromDate() !=null && paymentSearchCriteria.getFromDate()>0 ) && (paymentSearchCriteria.getToDate() !=null && paymentSearchCriteria.getToDate()>0 ) )
        {
        	searchCriteria.setToDate(paymentSearchCriteria.getToDate());
        	searchCriteria.setFromDate(paymentSearchCriteria.getFromDate());

        }
        
        List<String> ids = paymentRepository.fetchPaymentIds(searchCriteria);
        if (ids.isEmpty())
            return Collections.emptyList();

        PaymentSearchCriteria criteria = PaymentSearchCriteria.builder().ids(new HashSet<String>(ids)).build();
        return paymentRepository.fetchPaymentsForPlainSearch(criteria);
    }

    
    
    @Transactional
    public Payment createPaymentForWSMigration(PaymentRequest paymentRequest) {
    	
        paymentEnricher.enrichPaymentPreValidate(paymentRequest,true);
        paymentValidator.validatePaymentForCreateWSMigration(paymentRequest);
        paymentEnricher.enrichPaymentPostValidate(paymentRequest);

        Payment payment = paymentRequest.getPayment();
        Map<String, Bill> billIdToApportionedBill = apportionerService.apportionBill(paymentRequest);
        paymentEnricher.enrichAdvanceTaxHead(new LinkedList<>(billIdToApportionedBill.values()));
        setApportionedBillsToPayment(billIdToApportionedBill,payment);

        String payerId = createUser(paymentRequest);
        if(!StringUtils.isEmpty(payerId))
            payment.setPayerId(payerId);
        paymentRepository.savePayment(payment);

       // producer.producer(applicationProperties.getCreatePaymentTopicName(), paymentRequest);


        return payment;
    }
    
}
