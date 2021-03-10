package org.egov.collection.util;

import org.apache.commons.lang3.StringUtils;
import org.egov.collection.model.Payment;
import org.egov.collection.model.PaymentSearchCriteria;
import org.egov.collection.model.RequestInfoWrapper;
import org.egov.common.contract.request.RequestInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Set;

@Component
public class PropertyPaymentFilterTemp {

    @Autowired
    private RestTemplate restTemplate;


    @Value("${egov.property.host}")
    private String ptHost;

    @Value("${egov.property.validate.endpoint}")
    private String ptEndpoint;

    public Boolean isUserAuthorized(RequestInfo requestInfo, PaymentSearchCriteria paymentSearchCriteria, List<Payment> payments){

        if(!CollectionUtils.isEmpty(payments)){
            if(!payments.get(0).getPaymentDetails().get(0).getBusinessService().equalsIgnoreCase("PT"))
                return true;
        }

        Set<String> consumerCodes = paymentSearchCriteria.getConsumerCodes();
        String tenantId = paymentSearchCriteria.getTenantId();
        String url = getPTAuthorizeURL(tenantId, consumerCodes);

        RequestInfoWrapper wrapper = RequestInfoWrapper.builder().requestInfo(requestInfo).build();
        Boolean response = restTemplate.postForObject(url, wrapper, Boolean.class);

        return response;

    }


    private String getPTAuthorizeURL(String tenantId, Set<String> consumerCodes){

            StringBuilder url = new StringBuilder(ptHost);
            url.append(ptEndpoint);
            url.append("?");
            url.append("tenantId=");
            url.append(tenantId);
            url.append("&");
            url.append("propertyIds=");
            url.append(StringUtils.join(consumerCodes,","));
            return url.toString();


    }

}
