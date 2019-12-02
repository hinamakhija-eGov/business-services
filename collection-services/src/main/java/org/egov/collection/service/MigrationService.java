package org.egov.collection.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import lombok.extern.slf4j.Slf4j;
import org.egov.collection.config.ApplicationProperties;
import org.egov.collection.model.*;
import org.egov.collection.model.enums.InstrumentStatusEnum;
import org.egov.collection.model.enums.PaymentModeEnum;
import org.egov.collection.model.enums.PaymentStatusEnum;
import org.egov.collection.model.v1.*;
import org.egov.collection.producer.CollectionProducer;
import org.egov.collection.repository.ServiceRequestRepository;
import org.egov.collection.service.v1.CollectionService_v1;
import org.egov.collection.web.contract.Bill;
import org.egov.collection.web.contract.BillAccountDetail;
import org.egov.collection.web.contract.BillDetail;
import org.egov.common.contract.request.RequestInfo;
import org.egov.common.contract.response.ResponseInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.util.UriComponentsBuilder;

import java.math.BigDecimal;
import java.util.*;

import static org.egov.collection.model.enums.InstrumentTypesEnum.CARD;
import static org.egov.collection.model.enums.InstrumentTypesEnum.ONLINE;

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


    public void migrate(RequestInfo requestInfo, Integer batchSize){
        Integer offset = 0;
        while(true){
            ReceiptSearchCriteria_v1 criteria_v1 = ReceiptSearchCriteria_v1.builder()
                    .offset(offset).limit(batchSize).build();
            List<Receipt_v1> receipts = collectionService.fetchReceipts(criteria_v1);
            if(CollectionUtils.isEmpty(receipts))
                break;
            migrateReceipt(requestInfo, receipts);
            offset += batchSize;
        }
        log.info("Total receipts migrated: " + offset);
    }

    public void migrateReceipt(RequestInfo requestInfo, List<Receipt_v1> receipts){
        List<Payment> paymentList = new ArrayList<Payment>();
        for(Receipt_v1 receipt : receipts){
            Payment payment = getPayment(requestInfo,receipt);
            paymentList.add(payment);
        }
        PaymentResponse paymentResponse = new PaymentResponse(new ResponseInfo(), paymentList);
        producer.producer(properties.getCreateReceiptTopicName(), properties
                .getCreatePaymentTopicName(), paymentResponse);
    }
    

    private Payment getPayment(RequestInfo requestInfo,Receipt_v1 receipt){

        Payment payment = new Payment();
        payment.setTenantId(receipt.getTenantId());
        BigDecimal totalAmount = BigDecimal.valueOf(0);
        BigDecimal totalAmountPaid = BigDecimal.valueOf(0);
        //calculating TotalDue amount
        for(Bill_v1 bill: receipt.getBill()){
            for(BillDetail_v1 billdetail: bill.getBillDetails()){
                totalAmount = totalAmount.add(billdetail.getTotalAmount());
                totalAmountPaid = totalAmountPaid.add(billdetail.getAmountPaid());
            }
        }
        payment.setTotalDue(totalAmount.subtract(totalAmountPaid));
        payment.setTotalAmountPaid(totalAmountPaid);
        payment.setTransactionNumber(receipt.getInstrument().getTransactionNumber());
        payment.setTransactionDate(receipt.getReceiptDate());
        payment.setPaymentMode(PaymentModeEnum.fromValue(receipt.getInstrument().getInstrumentType().getName()));
        payment.setInstrumentDate(receipt.getInstrument().getInstrumentDate());
        payment.setInstrumentNumber(receipt.getInstrument().getInstrumentNumber());
        payment.setInstrumentStatus(receipt.getInstrument().getInstrumentStatus());
        //payment.setInstrumentStatus(InstrumentStatusEnum.fromValue("NEW"));
        payment.setIfscCode(receipt.getInstrument().getIfscCode());

        AuditDetails auditDetails = getAuditDetail(receipt.getAuditDetails());
        payment.setAuditDetails(auditDetails);
        payment.setAdditionalDetails((JsonNode)receipt.getBill().get(0).getAdditionalDetails());

        PaymentDetail paymentDetail = getPaymentDetail(receipt, totalAmount, totalAmountPaid, auditDetails,requestInfo);
        payment.setPaymentDetails(Arrays.asList(paymentDetail));

        payment.setPaidBy(receipt.getBill().get(0).getPaidBy());
        payment.setMobileNumber(receipt.getBill().get(0).getMobileNumber());
        payment.setPayerName(receipt.getBill().get(0).getPayerName());
        payment.setPayerAddress(receipt.getBill().get(0).getPayerAddress());
        payment.setPayerEmail(receipt.getBill().get(0).getPayerEmail());
        payment.setPayerId(receipt.getBill().get(0).getPayerId());

        if ((payment.getPaymentMode().toString()).equalsIgnoreCase(ONLINE.name()) ||
                payment.getPaymentMode().toString().equalsIgnoreCase(CARD.name()))
            payment.setPaymentStatus(PaymentStatusEnum.DEPOSITED);
        else
            payment.setPaymentStatus(PaymentStatusEnum.NEW);

        String id = UUID.randomUUID().toString();
        payment.setId(id);
        payment.getPaymentDetails().get(0).setId(id);


        return payment;

    }

    private PaymentDetail getPaymentDetail(Receipt_v1 receipt, BigDecimal totalAmount, BigDecimal totalAmountPaid, AuditDetails auditDetails,RequestInfo requestInfo){
        PaymentDetail paymentDetail = new PaymentDetail();

       // paymentDetail.setId(UUID.randomUUID().toString());
        paymentDetail.setTenantId(receipt.getTenantId());
        paymentDetail.setTotalDue(totalAmount.subtract(totalAmountPaid));
        paymentDetail.setTotalAmountPaid(totalAmountPaid);
        paymentDetail.setReceiptNumber(receipt.getReceiptNumber());
        paymentDetail.setManualReceiptNumber(receipt.getBill().get(0).getBillDetails().get(0).getManualReceiptNumber());
        paymentDetail.setManualReceiptDate(receipt.getBill().get(0).getBillDetails().get(0).getManualReceiptDate());
        paymentDetail.setReceiptDate(receipt.getReceiptDate());
        paymentDetail.setReceiptType(receipt.getBill().get(0).getBillDetails().get(0).getReceiptType());
        paymentDetail.setBusinessService(receipt.getBill().get(0).getBillDetails().get(0).getBusinessService());
        String billId = getBillId(receipt.getBill().get(0),requestInfo);
        paymentDetail.setBillId(billId);

        Bill bill = getBill(receipt,billId,totalAmount,totalAmountPaid,auditDetails);
        paymentDetail.setBill(bill);

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

    private Bill getBill(Receipt_v1 receipt, String billId, BigDecimal totalAmount, BigDecimal totalAmountPaid, AuditDetails auditDetails ){
        Bill newBill = new Bill();
        Bill_v1 oldBill = receipt.getBill().get(0);

        newBill.setId(billId);
        newBill.setMobileNumber(oldBill.getMobileNumber());
        newBill.setPaidBy(oldBill.getPaidBy());
        newBill.setPayerName(oldBill.getPayerName());
        newBill.setPayerAddress(oldBill.getPayerAddress());
        newBill.setPayerEmail(oldBill.getPayerEmail());
        newBill.setPayerId(oldBill.getPayerId());
        newBill.setStatus(Bill.StatusEnum.fromValue(oldBill.getBillDetails().get(0).getStatus()));
        newBill.setReasonForCancellation(oldBill.getBillDetails().get(0).getReasonForCancellation());
        newBill.setIsCancelled(oldBill.getIsCancelled());
        newBill.setAdditionalDetails((JsonNode) oldBill.getAdditionalDetails());

        BillDetail billDetails = getBillDetail(receipt, billId,auditDetails);
        newBill.setBillDetails(Arrays.asList(billDetails));

        newBill.setTenantId(oldBill.getTenantId());
        newBill.setAuditDetails(auditDetails);
        newBill.setCollectionModesNotAllowed(oldBill.getBillDetails().get(0).getCollectionModesNotAllowed());
        newBill.setPartPaymentAllowed(oldBill.getBillDetails().get(0).getPartPaymentAllowed());
        newBill.setIsAdvanceAllowed(oldBill.getBillDetails().get(0).getIsAdvanceAllowed());
        newBill.setMinimumAmountToBePaid(oldBill.getBillDetails().get(0).getMinimumAmount());
        newBill.setBusinessService(oldBill.getBillDetails().get(0).getBusinessService());
        newBill.setTotalAmount(totalAmount);
        newBill.setConsumerCode(oldBill.getBillDetails().get(0).getConsumerCode());
        newBill.setBillNumber(oldBill.getBillDetails().get(0).getBillNumber());
        newBill.setBillDate(oldBill.getBillDetails().get(0).getBillDate());
        newBill.setAmountPaid(totalAmountPaid);



        return newBill;

    }

    private BillDetail getBillDetail(Receipt_v1 receipt, String billId, AuditDetails auditDetails){
        BillDetail newBillDetail = new BillDetail();
        BillDetail_v1 oldBillDetail = receipt.getBill().get(0).getBillDetails().get(0);

        newBillDetail.setBillDescription(oldBillDetail.getBillDescription());
        newBillDetail.setDisplayMessage(oldBillDetail.getDisplayMessage());
        newBillDetail.setCallBackForApportioning(oldBillDetail.getCallBackForApportioning());
        newBillDetail.setCancellationRemarks(oldBillDetail.getCancellationRemarks());
        newBillDetail.setId(oldBillDetail.getId());
        newBillDetail.setId(oldBillDetail.getId());
        newBillDetail.setTenantId(oldBillDetail.getTenantId());
        newBillDetail.setDemandId(oldBillDetail.getDemandId());
        newBillDetail.setBillId(billId);
        newBillDetail.setAmount(oldBillDetail.getTotalAmount());
        newBillDetail.setAmountPaid(oldBillDetail.getAmountPaid());
        newBillDetail.setFromPeriod(oldBillDetail.getFromPeriod());
        newBillDetail.setToPeriod(oldBillDetail.getToPeriod());
        newBillDetail.setAdditionalDetails(oldBillDetail.getAdditionalDetails());
        newBillDetail.setChannel(oldBillDetail.getChannel());
        newBillDetail.setVoucherHeader(oldBillDetail.getVoucherHeader());
        newBillDetail.setBoundary(oldBillDetail.getBoundary());
        newBillDetail.setManualReceiptNumber(oldBillDetail.getManualReceiptNumber());
        newBillDetail.setManualReceiptDate(oldBillDetail.getManualReceiptDate());

        List<BillAccountDetail> billAccountDetail = getBillAccountDetail(oldBillDetail.getBillAccountDetails(), auditDetails);
        newBillDetail.setBillAccountDetails(billAccountDetail);


        newBillDetail.setCollectionType(oldBillDetail.getCollectionType());
        newBillDetail.setAuditDetails(auditDetails);
        newBillDetail.setExpiryDate(oldBillDetail.getExpiryDate());


        return newBillDetail;
    }

    private List<BillAccountDetail> getBillAccountDetail(List<BillAccountDetail_v1> billAccountDetails, AuditDetails auditDetails){
        List<BillAccountDetail> newBillAccountDetails= new ArrayList<BillAccountDetail>();

        for(BillAccountDetail_v1 oldBillAccountDetail : billAccountDetails){
            BillAccountDetail newBillAccountDetail = new BillAccountDetail();
            newBillAccountDetail.setId(oldBillAccountDetail.getId());
            newBillAccountDetail.setTenantId(oldBillAccountDetail.getTenantId());
            newBillAccountDetail.setBillDetailId(oldBillAccountDetail.getBillDetail());
            newBillAccountDetail.setDemandDetailId(oldBillAccountDetail.getDemandDetailId());
            newBillAccountDetail.setOrder(oldBillAccountDetail.getOrder());
            newBillAccountDetail.setAmount(oldBillAccountDetail.getAmount());
            newBillAccountDetail.setAdjustedAmount(oldBillAccountDetail.getAdjustedAmount());
            newBillAccountDetail.setIsActualDemand(oldBillAccountDetail.getIsActualDemand());
            newBillAccountDetail.setTaxHeadCode(oldBillAccountDetail.getTaxHeadCode());
            newBillAccountDetail.setAdditionalDetails(oldBillAccountDetail.getAdditionalDetails());
            newBillAccountDetail.setPurpose(oldBillAccountDetail.getPurpose());
            newBillAccountDetail.setAuditDetails(auditDetails);
            newBillAccountDetails.add(newBillAccountDetail);
        }

        return  newBillAccountDetails;
    }




    private String getBillId(Bill_v1 bill,RequestInfo requestInfo){
        String billNumber = bill.getBillDetails().get(0).getBillNumber();
        String tenantId = bill.getBillDetails().get(0).getTenantId();
        String service = bill.getBillDetails().get(0).getBusinessService();
        String status = bill.getBillDetails().get(0).getStatus();

        StringBuilder url = getBillSearchURI(tenantId,billNumber,service,status);

        RequestInfoWrapper requestInfoWrapper = RequestInfoWrapper.builder().requestInfo(requestInfo).build();

        Object response = serviceRequestRepository.fetchResult(url,requestInfoWrapper);
        ObjectMapper mapper = new ObjectMapper();
        try{
            String billId = JsonPath.read(mapper.writeValueAsString(response), "$.Bill[0].id");
            return billId;
        }catch(Exception e){
            return null;
        }


    }


    private StringBuilder getBillSearchURI(String tenantId, String billNumber, String service,String status){
        StringBuilder builder = new StringBuilder(properties.getBillingServiceHostName());
        builder.append(properties.getSearchBill()).append("?");
        builder.append("tenantId=").append(tenantId);
        builder.append("&service=").append(service);
        builder.append("&billNumber=").append(billNumber);


        /*
        String uri = UriComponentsBuilder
                .fromHttpUrl(properties.getBillingServiceHostName())
                .path(properties.getSearchBill()).queryParam(tenantId,billNumber)
                .build()
                .toUriString();*/
        return  builder;

    }


}
