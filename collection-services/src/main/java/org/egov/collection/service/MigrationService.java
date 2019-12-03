package org.egov.collection.service;

import static org.egov.collection.model.enums.InstrumentTypesEnum.CARD;
import static org.egov.collection.model.enums.InstrumentTypesEnum.ONLINE;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.egov.collection.config.ApplicationProperties;
import org.egov.collection.model.AuditDetails;
import org.egov.collection.model.Payment;
import org.egov.collection.model.PaymentDetail;
import org.egov.collection.model.PaymentResponse;
import org.egov.collection.model.RequestInfoWrapper;
import org.egov.collection.model.enums.PaymentModeEnum;
import org.egov.collection.model.enums.PaymentStatusEnum;
import org.egov.collection.model.v1.AuditDetails_v1;
import org.egov.collection.model.v1.Bill_v1;
import org.egov.collection.model.v1.ReceiptSearchCriteria_v1;
import org.egov.collection.model.v1.Receipt_v1;
import org.egov.collection.producer.CollectionProducer;
import org.egov.collection.repository.ServiceRequestRepository;
import org.egov.collection.service.v1.CollectionService_v1;
import org.egov.collection.web.contract.Bill;
import org.egov.collection.web.contract.BillDetail;
import org.egov.collection.web.contract.BillResponse;
import org.egov.common.contract.request.RequestInfo;
import org.egov.common.contract.response.ResponseInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class MigrationService {


    private ApplicationProperties properties;

    private ServiceRequestRepository serviceRequestRepository;

    private CollectionProducer producer;

    @Autowired
    private CollectionService_v1 collectionService;

    @Autowired
    public MigrationService(ApplicationProperties properties, ServiceRequestRepository serviceRequestRepository,CollectionProducer producer) {
        this.properties = properties;
        this.serviceRequestRepository = serviceRequestRepository;
        this.producer = producer;
    }


    public void migrate(RequestInfo requestInfo, Integer batchSize) throws JsonProcessingException {
        Integer offset = 0;
        Map<String, Long> billAndBillDetails = new HashMap<>();
        while(true){
            ReceiptSearchCriteria_v1 criteria_v1 = ReceiptSearchCriteria_v1.builder()
                    .offset(offset).limit(batchSize).build();
            List<Receipt_v1> receipts = collectionService.fetchReceipts(criteria_v1);
            if(CollectionUtils.isEmpty(receipts))
                break;
            migrateReceipt(requestInfo, receipts, billAndBillDetails);
            offset += batchSize;
        }
        log.info("BillAndBillDetails: "+billAndBillDetails);
        log.info("Total receipts migrated: " + offset);
    }

    public void migrateReceipt(RequestInfo requestInfo, List<Receipt_v1> receipts, Map<String, Long> billAndBillDetails){
        List<Payment> paymentList = new ArrayList<Payment>();
        for(Receipt_v1 receipt : receipts){
            Payment payment = transformToPayment(requestInfo,receipt, billAndBillDetails);
            if(null != payment){
                paymentList.add(payment);
            }
        }
        PaymentResponse paymentResponse = new PaymentResponse(new ResponseInfo(), paymentList);
        producer.producer(properties.getCollectionMigrationTopicName(), properties
                .getCollectionMigrationTopicKey(), paymentResponse);
    }

    private Payment transformToPayment(RequestInfo requestInfo, Receipt_v1 receipt, Map<String, Long> billAndBillDetails) {
    	Bill bill = getBillFromV2(receipt.getBill().get(0),requestInfo);
        if(null == bill) 
            return null;
        else {
        	if(null != billAndBillDetails.get(bill.getId()))
        		billAndBillDetails.put(bill.getId(), billAndBillDetails.get(bill.getId()) + 1L);
        	else
        		billAndBillDetails.put(bill.getId(), 1L);
        	
        	return getPayment(requestInfo, receipt, bill);
        }
    }
    

    private Payment getPayment(RequestInfo requestInfo, Receipt_v1 receipt, Bill newBill){

        Payment payment = new Payment();

        BigDecimal totalAmount = newBill.getTotalAmount();
        BigDecimal totalAmountPaid = receipt.getInstrument().getAmount();
        newBill.setAmountPaid(totalAmountPaid);
        
        for(BillDetail billDetail : newBill.getBillDetails()) {
        	billDetail.setAmountPaid(totalAmountPaid);
        }
        
        payment.setId(UUID.randomUUID().toString());
        payment.setTenantId(receipt.getTenantId());
        payment.setTotalDue(totalAmount.subtract(totalAmountPaid));
        payment.setTotalAmountPaid(totalAmountPaid);
        payment.setTransactionNumber(receipt.getInstrument().getTransactionNumber());
        payment.setTransactionDate(receipt.getReceiptDate());
        payment.setPaymentMode(PaymentModeEnum.fromValue(receipt.getInstrument().getInstrumentType().getName()));
        payment.setInstrumentDate(receipt.getInstrument().getInstrumentDate());
        payment.setInstrumentNumber(receipt.getInstrument().getInstrumentNumber());
        payment.setInstrumentStatus(receipt.getInstrument().getInstrumentStatus());
        payment.setIfscCode(receipt.getInstrument().getIfscCode());
        payment.setPaidBy(receipt.getBill().get(0).getPaidBy());
        payment.setPayerName(receipt.getBill().get(0).getPayerName());
        payment.setPayerAddress(receipt.getBill().get(0).getPayerAddress());
        payment.setPayerEmail(receipt.getBill().get(0).getPayerEmail());
        payment.setPayerId(receipt.getBill().get(0).getPayerId());
        
        if(receipt.getBill().get(0).getMobileNumber() == null){
            payment.setMobileNumber("NA");
        }else{
            payment.setMobileNumber(receipt.getBill().get(0).getMobileNumber());
        }
        
        if ((payment.getPaymentMode().toString()).equalsIgnoreCase(ONLINE.name()) ||
                payment.getPaymentMode().toString().equalsIgnoreCase(CARD.name()))
            payment.setPaymentStatus(PaymentStatusEnum.DEPOSITED);
        else
            payment.setPaymentStatus(PaymentStatusEnum.NEW);


        AuditDetails auditDetails = getAuditDetail(receipt.getAuditDetails());
        payment.setAuditDetails(auditDetails);
        payment.setAdditionalDetails((JsonNode)receipt.getBill().get(0).getAdditionalDetails());

        PaymentDetail paymentDetail = getPaymentDetail(receipt, auditDetails, requestInfo);
    	
        paymentDetail.setBill(newBill);
        paymentDetail.setPaymentId(payment.getId());
    	paymentDetail.setBillId(newBill.getId());
        paymentDetail.setTotalDue(totalAmount.subtract(totalAmountPaid));
        paymentDetail.setTotalAmountPaid(totalAmountPaid);
        payment.setPaymentDetails(Arrays.asList(paymentDetail));

        return payment;

    }

    private PaymentDetail getPaymentDetail(Receipt_v1 receipt, AuditDetails auditDetails, RequestInfo requestInfo){
        
        PaymentDetail paymentDetail = new PaymentDetail();

        paymentDetail.setId(UUID.randomUUID().toString());
        paymentDetail.setTenantId(receipt.getTenantId());
        paymentDetail.setReceiptNumber(receipt.getReceiptNumber());
        paymentDetail.setManualReceiptNumber(receipt.getBill().get(0).getBillDetails().get(0).getManualReceiptNumber());
        paymentDetail.setManualReceiptDate(receipt.getBill().get(0).getBillDetails().get(0).getManualReceiptDate());
        paymentDetail.setReceiptDate(receipt.getReceiptDate());
        paymentDetail.setReceiptType(receipt.getBill().get(0).getBillDetails().get(0).getReceiptType());
        paymentDetail.setBusinessService(receipt.getBill().get(0).getBillDetails().get(0).getBusinessService());

        paymentDetail.setAuditDetails(auditDetails);
        paymentDetail.setAdditionalDetails((JsonNode)receipt.getBill().get(0).getAdditionalDetails());

        return paymentDetail;

    }

    private AuditDetails getAuditDetail(AuditDetails_v1 oldAuditDetails){
        AuditDetails newAuditDetails = new AuditDetails();
        newAuditDetails.setCreatedBy(oldAuditDetails.getCreatedBy());
        newAuditDetails.setCreatedTime(oldAuditDetails.getCreatedDate());
        newAuditDetails.setLastModifiedBy(oldAuditDetails.getLastModifiedBy());
        newAuditDetails.setLastModifiedTime(oldAuditDetails.getLastModifiedDate());
        return newAuditDetails;
    }

    private Bill getBillFromV2(Bill_v1 bill,RequestInfo requestInfo){
            String billNumber = bill.getBillDetails().get(0).getBillNumber();
            String tenantId = bill.getBillDetails().get(0).getTenantId();
            String service = bill.getBillDetails().get(0).getBusinessService();
            String status = bill.getBillDetails().get(0).getStatus();

            StringBuilder url = getBillSearchURI(tenantId,billNumber,service,status);

            RequestInfoWrapper requestInfoWrapper = RequestInfoWrapper.builder().requestInfo(requestInfo).build();

            Object response = serviceRequestRepository.fetchResult(url,requestInfoWrapper);
            ObjectMapper mapper = new ObjectMapper();
            try{
                BillResponse billResponse = mapper.convertValue(response, BillResponse.class);
                if(CollectionUtils.isEmpty(billResponse.getBill())){
                    log.info("No bills for billNumber: "+billNumber);
                    return null;
                }else{
                    Bill newBill = billResponse.getBill().get(0);
                    if(null == newBill.getStatus())
                        newBill.setStatus(Bill.StatusEnum.EXPIRED);
                    
                    return newBill;
                }
            }catch(Exception e) {
                log.error("Exception: ",e);
                return null;
            }

    }


    private StringBuilder getBillSearchURI(String tenantId, String billNumber, String service,String status){
        StringBuilder builder = new StringBuilder(properties.getBillingServiceHostName());
        builder.append(properties.getSearchBill()).append("?");
        builder.append("tenantId=").append(tenantId);
        builder.append("&service=").append(service);
        builder.append("&billNumber=").append(billNumber);

        return  builder;

    }


}
