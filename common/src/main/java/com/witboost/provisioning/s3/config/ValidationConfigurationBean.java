package com.witboost.provisioning.s3.config;

import com.witboost.provisioning.framework.service.validation.ValidationConfiguration;
import com.witboost.provisioning.s3.service.validation.StorageAreaValidationService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ValidationConfigurationBean {

    @Bean
    ValidationConfiguration validationConfiguration(StorageAreaValidationService storageAreaValidationService) {
        return ValidationConfiguration.builder()
                .storageValidationService(storageAreaValidationService)
                .build();
    }
}
