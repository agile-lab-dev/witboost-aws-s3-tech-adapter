package com.witboost.provisioning.s3.config;

import com.witboost.provisioning.framework.service.ProvisionConfiguration;
import com.witboost.provisioning.s3.client.BucketManager;
import com.witboost.provisioning.s3.service.provision.StorageAreaProvisionService;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Configuration
public class ProvisionConfigurationBean {

    private final Map<Region, S3Client> s3ClientCache = new ConcurrentHashMap<>();

    @Autowired
    private BucketManager bucketManager;

    @Bean
    public StorageAreaProvisionService storageAreaProvisionService(BucketManager bucketManager) {
        return new StorageAreaProvisionService(this::getS3Client, bucketManager);
    }

    @Bean
    @Primary
    ProvisionConfiguration provisionConfiguration(StorageAreaProvisionService storageAreaProvisionService) {
        return ProvisionConfiguration.builder()
                .storageProvisionService(storageAreaProvisionService)
                .build();
    }

    protected S3Client getS3Client(Region region) {
        return s3ClientCache.computeIfAbsent(
                region, r -> S3Client.builder().region(r).build());
    }
}
