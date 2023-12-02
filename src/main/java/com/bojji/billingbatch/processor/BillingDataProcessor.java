package com.bojji.billingbatch.processor;

import com.bojji.billingbatch.domain.BillingData;
import com.bojji.billingbatch.domain.ReportingData;
import com.bojji.billingbatch.service.PricingService;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;

public class BillingDataProcessor implements ItemProcessor<BillingData, ReportingData> {
    @Value("${spring.cellular.spending.threshold:150}")
    private float spendingThreshold;
    private final PricingService pricingService;
    public BillingDataProcessor(PricingService pricingService) {
        this.pricingService = pricingService;
    }
    @Override
    public ReportingData process(BillingData billingData) throws Exception {
        double billingTotal = billingData.dataUsage() * pricingService.getDataPricing() + billingData.callDuration() * pricingService.getCallPricing() + billingData.smsCount() * pricingService.getSmsPricing();

        if(billingTotal < spendingThreshold)
            return null;

        return new ReportingData(billingData, billingTotal);
    }
}
