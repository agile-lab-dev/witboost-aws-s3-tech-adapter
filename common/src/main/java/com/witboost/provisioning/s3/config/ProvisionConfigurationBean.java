package com.witboost.provisioning.s3.config;

import com.witboost.provisioning.framework.service.ProvisionConfiguration;
import com.witboost.provisioning.s3.service.provision.StorageAreaProvisionService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ProvisionConfigurationBean {

    @Bean
    ProvisionConfiguration provisionConfiguration(StorageAreaProvisionService storageAreaProvisionService) {
        return ProvisionConfiguration.builder()
                .storageProvisionService(storageAreaProvisionService)
                .build();
    }
}
