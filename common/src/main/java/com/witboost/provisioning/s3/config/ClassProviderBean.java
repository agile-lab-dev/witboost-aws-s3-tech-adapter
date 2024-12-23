package com.witboost.provisioning.s3.config;

import com.witboost.provisioning.framework.service.ComponentClassProvider;
import com.witboost.provisioning.framework.service.SpecificClassProvider;
import com.witboost.provisioning.framework.service.impl.ComponentClassProviderImpl;
import com.witboost.provisioning.framework.service.impl.SpecificClassProviderImpl;
import com.witboost.provisioning.model.Specific;
import com.witboost.provisioning.model.StorageArea;
import com.witboost.provisioning.s3.model.S3Specific;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ClassProviderBean {

    @Bean
    public SpecificClassProvider specificClassProvider() {
        return SpecificClassProviderImpl.builder()
                .withDefaultSpecificClass(S3Specific.class)
                .withDefaultReverseProvisionSpecificClass(Specific.class)
                .build();
    }

    @Bean
    public ComponentClassProvider componentClassProvider() {
        return ComponentClassProviderImpl.builder()
                .withDefaultClass(StorageArea.class)
                .build();
    }
}
