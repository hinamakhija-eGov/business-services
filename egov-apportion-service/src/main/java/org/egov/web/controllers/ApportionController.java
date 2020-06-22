package org.egov.web.controllers;


import org.egov.service.ApportionService;
import org.egov.service.apportions.OrderByPriority;
import org.egov.util.ResponseInfoFactory;
import org.egov.web.models.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.*;
import org.egov.web.models.enums.DemandApportionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.bind.annotation.RequestMapping;
import java.io.IOException;
import java.util.*;

    import javax.validation.constraints.*;
    import javax.validation.Valid;
    import javax.servlet.http.HttpServletRequest;
        import java.util.Optional;
@javax.annotation.Generated(value = "org.egov.codegen.SpringBootCodegen", date = "2019-02-25T15:07:36.183+05:30")

@Controller
    @RequestMapping("/v1")
    public class ApportionController {

        private final ObjectMapper objectMapper;

        private ApportionService apportionService;

        private ResponseInfoFactory responseInfoFactory;



    @Autowired
    public ApportionController(ObjectMapper objectMapper, ApportionService apportionService, ResponseInfoFactory responseInfoFactory) {
        this.objectMapper = objectMapper;
        this.apportionService = apportionService;
        this.responseInfoFactory = responseInfoFactory;
    }




    /**
     * Executes the apportioning process on the given bills
     * @param apportionRequest The ApportionRequest containing the bill to be apportioned
     * @return Apportioned Bills
     */
    @RequestMapping(value="/_apportion", method = RequestMethod.POST)
        public ResponseEntity<ApportionResponse> apportionPost(@Valid @RequestBody ApportionRequest apportionRequest){
            List<Bill> billInfos = apportionService.apportionBills(apportionRequest);
            ApportionResponse response = ApportionResponse.builder()
                    .tenantId(apportionRequest.getTenantId())
                    .bills(billInfos)
                    .responseInfo(responseInfoFactory.createResponseInfoFromRequestInfo(apportionRequest.getRequestInfo(),
                            true)).build();
        return new ResponseEntity<>(response,HttpStatus.OK);
        }

    @RequestMapping(value="/_apportionV2", method = RequestMethod.POST)
    public ResponseEntity<ApportionDemandResponse> apportionPost(@Valid @RequestBody DemandApportionRequest apportionRequest){
        List<Demand> demands = apportionService.apportionDemands(apportionRequest);
        ApportionDemandResponse response = ApportionDemandResponse.builder()
                .tenantId(apportionRequest.getTenantId())
                .demands(demands)
                .responseInfo(responseInfoFactory.createResponseInfoFromRequestInfo(apportionRequest.getRequestInfo(),
                        true)).build();
        return new ResponseEntity<>(response,HttpStatus.OK);
    }





    }
