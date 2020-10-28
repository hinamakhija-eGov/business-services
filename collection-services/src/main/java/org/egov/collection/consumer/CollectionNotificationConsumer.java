package org.egov.collection.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.egov.collection.model.Payment;
import org.egov.collection.web.contract.Bill;
import org.egov.collection.model.PaymentDetail;
import org.egov.collection.model.PaymentRequest;
import org.egov.collection.producer.CollectionProducer;
import org.egov.common.contract.request.RequestInfo;
import org.egov.mdms.model.MdmsCriteria;
import org.egov.mdms.model.MdmsCriteriaReq;
import org.egov.mdms.model.MasterDetail;
import org.egov.mdms.model.ModuleDetail;
import org.egov.collection.config.ApplicationProperties;

import org.egov.tracer.model.CustomException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;

import lombok.extern.slf4j.Slf4j;
import net.logstash.logback.encoder.org.apache.commons.lang.StringUtils;

import static org.egov.collection.config.CollectionServiceConstants.*;

@Slf4j
@Component
public class CollectionNotificationConsumer{

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ApplicationProperties applicationProperties;

    @Autowired
    private CollectionProducer producer;

    @Autowired
    private RestTemplate restTemplate;

    @KafkaListener(topics = {"${kafka.topics.payment.create.name}",
            "${kafka.topics.payment.cancel.name}",})
    public void listen(HashMap<String, Object> record, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic){

        try{
            PaymentRequest req = objectMapper.convertValue(record, PaymentRequest.class);
            sendNotification(req);
        }catch(Exception e){
            log.error("Exception while reading from the queue: ", e);
        }

    }

    private void sendNotification(PaymentRequest paymentReq){
        Payment payment = paymentReq.getPayment();
        List<String> businessServiceAllowed = fetchBusinessServiceFromMDMS(paymentReq.getRequestInfo(), paymentReq.getPayment().getTenantId());
        if(!CollectionUtils.isEmpty(businessServiceAllowed)){
            for(PaymentDetail detail : payment.getPaymentDetails()){
                Bill bill = detail.getBill();

                String mobNo = bill.getMobileNumber();
                String paymentStatus = payment.getPaymentStatus().toString();

                String message = buildSmsBody(bill, detail, paymentReq.getRequestInfo(), paymentStatus);
                if(!StringUtils.isEmpty(message)){
                    Map<String, Object> request = new HashMap<>();
                    request.put("mobileNumber", mobNo);
                    request.put("message", message);

                    producer.producer(applicationProperties.getSmsTopic(), applicationProperties.getSmsTopickey(), request);
                }
                else{
                    log.error("Message not configured! No notification will be sent.");
                }
            }
        }
        else{
            log.info("Business services to which notifications are to be sent, couldn't be retrieved! Notifications will not be sent.");
        }
    }

    private String buildSmsBody(Bill bill, PaymentDetail paymentDetail, RequestInfo requestInfo, String paymentStatus){
        String message = null;
        String content = null;
        switch(paymentStatus.toUpperCase()){
            case "NEW":
                content = fetchContentFromLocalization(requestInfo, paymentDetail.getTenantId(), COLLECTION_LOCALIZATION_MODULE, WF_MT_STATUS_OPEN_CODE);
                break;
            case "DEPOSITED":
                content = fetchContentFromLocalization(requestInfo, paymentDetail.getTenantId(), COLLECTION_LOCALIZATION_MODULE, WF_MT_STATUS_DEPOSITED_CODE);
                break;
            case "CANCELLED":
                content = fetchContentFromLocalization(requestInfo, paymentDetail.getTenantId(), COLLECTION_LOCALIZATION_MODULE, WF_MT_STATUS_CANCELLED_CODE);
                break;
            case "DISHONOURED":
                content = fetchContentFromLocalization(requestInfo, paymentDetail.getTenantId(), COLLECTION_LOCALIZATION_MODULE, WF_MT_STATUS_DISHONOURED_CODE);
                break;
            default:
                break;
        }
        if(!StringUtils.isEmpty(content)){
            StringBuilder link = new StringBuilder();
            link.append(applicationProperties.getUiHost() + "/citizen").append("/otpLogin?mobileNo=").append(bill.getMobileNumber()).append("&redirectTo=")
                    .append(applicationProperties.getUiRedirectUrl()).append("&params=").append(paymentDetail.getTenantId() + "," + paymentDetail.getReceiptNumber());

            content = content.replaceAll("<rcpt_link>", link.toString());

            String moduleName = fetchContentFromLocalization(requestInfo, paymentDetail.getTenantId(),
                    BUSINESSSERVICE_LOCALIZATION_MODULE, formatCodes(paymentDetail.getBusinessService()));

            if(StringUtils.isEmpty(moduleName)) {
                moduleName = "Adhoc Tax";
            }

            content = content.replaceAll("<owner_name>", bill.getPaidBy());
            content = content.replaceAll("<mod_name>", moduleName);
            content = content.replaceAll("<rcpt_no>",  paymentDetail.getReceiptNumber());
            if(content.contains("<amount_paid>"))
                content = content.replaceAll("<amount_paid>", bill.getAmountPaid().toString());
            content = content.replaceAll("<unique_id>", bill.getConsumerCode());
            message = content;
        }
        return message;
    }

    private String fetchContentFromLocalization(RequestInfo requestInfo, String tenantId, String module, String code){
        String message = null;
        List<String> codes = new ArrayList<>();
        List<String> messages = new ArrayList<>();
        Object result = null;
        String locale = "";
        if(requestInfo.getMsgId().contains("|"))
            locale = requestInfo.getMsgId().split("[\\|]")[1];
        if(StringUtils.isEmpty(locale))
            locale = applicationProperties.getFallBackLocale();
        StringBuilder uri = new StringBuilder();
        uri.append(applicationProperties.getLocalizationHost()).append(applicationProperties.getLocalizationEndpoint());
        uri.append("?tenantId=").append(tenantId.split("\\.")[0]).append("&locale=").append(locale).append("&module=").append(module);
        Map<String, Object> request = new HashMap<>();
        request.put("RequestInfo", requestInfo);
        try {
            result = restTemplate.postForObject(uri.toString(), request, Map.class);
            codes = JsonPath.read(result, LOCALIZATION_CODES_JSONPATH);
            messages = JsonPath.read(result, LOCALIZATION_MSGS_JSONPATH);
        } catch (Exception e) {
            log.error("Exception while fetching from localization: " + e);
        }
        if(CollectionUtils.isEmpty(messages)){
            throw new CustomException("LOCALIZATION_NOT_FOUND", "Localization not found for the code: " + code);
        }
        for(int index = 0; index < codes.size(); index++){
            if(codes.get(index).equals(code)){
                message = messages.get(index);
            }
        }
        return message;
    }
    private List<String> fetchBusinessServiceFromMDMS(RequestInfo requestInfo, String tenantId){
        List<String> masterData = new ArrayList<>();
        StringBuilder uri = new StringBuilder();
        uri.append(applicationProperties.getMdmsHost()).append(applicationProperties.getMdmsUrl());
        if(StringUtils.isEmpty(tenantId))
            return masterData;
        MdmsCriteriaReq request = getRequestForEvents(requestInfo, tenantId.split("\\.")[0]);
        try {
            Object response = restTemplate.postForObject(uri.toString(), request, Map.class);
            masterData = JsonPath.read(response, BUSINESSSERVICE_CODES_JSONPATH);
        }catch(Exception e) {
            log.error("Exception while fetching business service codes: ",e);
        }
        return masterData;
    }

    private MdmsCriteriaReq getRequestForEvents(RequestInfo requestInfo, String tenantId) {
        MasterDetail masterDetail = org.egov.mdms.model.MasterDetail.builder()
                .name(BUSINESSSERVICE_MDMS_MASTER).filter(BUSINESSSERVICE_CODES_FILTER).build();
        List<MasterDetail> masterDetails = new ArrayList<>();
        masterDetails.add(masterDetail);
        ModuleDetail moduleDetail = ModuleDetail.builder().moduleName(BUSINESSSERVICE_MDMS_MODULE)
                .masterDetails(masterDetails).build();
        List<ModuleDetail> moduleDetails = new ArrayList<>();
        moduleDetails.add(moduleDetail);
        MdmsCriteria mdmsCriteria = MdmsCriteria.builder().tenantId(tenantId).moduleDetails(moduleDetails).build();
        return MdmsCriteriaReq.builder().requestInfo(requestInfo).mdmsCriteria(mdmsCriteria).build();
    }

    private String formatCodes(String code){
        String regexForSpecialCharacters = "[-+$&,:;=?@#|'<>.^*()%!]";
        code = code.replaceAll(regexForSpecialCharacters, "");
        code = code.replaceAll(" ", "_");

        return BUSINESSSERVICELOCALIZATION_CODE_PREFIX + code.toUpperCase();
    }
}
