package org.egov.service;

import org.egov.web.models.*;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

@Service
public class TranslationService {




    public ApportionRequestV2 translate(Bill bill){

        String businessService = bill.getBusinessService();
        BigDecimal amountPaid = bill.getAmountPaid();
        Boolean isAdvanceAllowed = bill.getIsAdvanceAllowed();

        ApportionRequestV2 apportionRequestV2 = ApportionRequestV2.builder().amountPaid(amountPaid).businessService(businessService)
                                                .isAdvanceAllowed(isAdvanceAllowed).build();

        List<BillDetail> billDetails = bill.getBillDetails();

        for(BillDetail billDetail : billDetails){

            TaxDetail taxDetail = TaxDetail.builder().fromPeriod(billDetail.getFromPeriod()).amountToBePaid(billDetail.getAmount())
                                  .amountPaid((billDetail.getAmountPaid() == null) ? BigDecimal.ZERO : billDetail.getAmountPaid())
                                  .build();

            billDetail.getBillAccountDetails().forEach(billAccountDetail -> {
                Bucket bucket = Bucket.builder().amount(billAccountDetail.getAmount())
                                .adjustedAmount((billAccountDetail.getAdjustedAmount()==null) ? BigDecimal.ZERO : billAccountDetail.getAdjustedAmount())
                                .taxHeadCode(billAccountDetail.getTaxHeadCode())
                                .priority(billAccountDetail.getOrder())
                                .build();
                taxDetail.addBucket(bucket);
            });

            apportionRequestV2.addTaxDetail(taxDetail);
        }

        return apportionRequestV2;

    }



    public ApportionRequestV2 translate(List<Demand> demands) {


        // Group by businessService before calling this function
        String businessService = demands.get(0).getBusinessService();

        // FIX ME
        BigDecimal amountPaid = BigDecimal.ZERO;
        Boolean isAdvanceAllowed = null;


        ApportionRequestV2 apportionRequestV2 = ApportionRequestV2.builder().amountPaid(amountPaid).businessService(businessService)
                .isAdvanceAllowed(isAdvanceAllowed).build();


        for(Demand demand : demands){

            TaxDetail taxDetail = TaxDetail.builder().fromPeriod(demand.getTaxPeriodFrom()).build();

            BigDecimal amountToBePaid = BigDecimal.ZERO;
            BigDecimal collectedAmount = BigDecimal.ZERO;

            for(DemandDetail demandDetail : demand.getDemandDetails()){

                Bucket bucket = Bucket.builder().amount(demandDetail.getTaxAmount())
                        .adjustedAmount((demandDetail.getCollectionAmount()==null) ? BigDecimal.ZERO : demandDetail.getCollectionAmount())
                        .taxHeadCode(demandDetail.getTaxHeadMasterCode())
                      //  .priority(demandDetail.getOrder())
                        .build();
                taxDetail.addBucket(bucket);


                amountToBePaid = amountToBePaid.add(demandDetail.getTaxAmount());
                collectedAmount = collectedAmount.add(demandDetail.getCollectionAmount());
            }

            taxDetail.setAmountPaid(collectedAmount);
            taxDetail.setAmountToBePaid(amountToBePaid);

            apportionRequestV2.addTaxDetail(taxDetail);


        }

        return apportionRequestV2;
    }




    }
