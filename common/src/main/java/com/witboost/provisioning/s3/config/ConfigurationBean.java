package com.witboost.provisioning.s3.config;

import com.witboost.provisioning.framework.service.ProvisionConfiguration;
import com.witboost.provisioning.framework.service.validation.ValidationConfiguration;
import com.witboost.provisioning.s3.client.BucketManager;
import com.witboost.provisioning.s3.service.provision.StorageAreaProvisionService;
import com.witboost.provisioning.s3.service.validation.StorageAreaValidationService;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

@Configuration
public class ConfigurationBean {

    private final Map<Region, S3Client> s3ClientCache = new ConcurrentHashMap<>();

    private final Map<Region, KmsClient> kmsClientCache = new ConcurrentHashMap<>();

    @Autowired
    BucketManager bucketManager;

    @Bean
    public StsClient stsClient() {
        return StsClient.create();
    }

    @Bean
    public StorageAreaProvisionService storageAreaProvisionService(StsClient stsClient) {
        return new StorageAreaProvisionService(this::getS3Client, this::getKmsClient, stsClient, bucketManager);
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

    protected KmsClient getKmsClient(Region region) {
        return kmsClientCache.computeIfAbsent(
                region, r -> KmsClient.builder().region(r).build());
    }

    @Bean
    StorageAreaValidationService storageAreaValidationService(BucketManager bucketManager) {
        return new StorageAreaValidationService(this::getS3Client, bucketManager);
    }

    @Bean
    ValidationConfiguration validationConfiguration(StorageAreaValidationService storageAreaValidationService) {
        return ValidationConfiguration.builder()
                .storageValidationService(storageAreaValidationService)
                .build();
    }
}
