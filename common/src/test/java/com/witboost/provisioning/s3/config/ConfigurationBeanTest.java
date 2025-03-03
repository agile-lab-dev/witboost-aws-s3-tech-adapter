package com.witboost.provisioning.s3.config;

import static org.junit.jupiter.api.Assertions.*;

import com.witboost.provisioning.framework.service.ProvisionConfiguration;
import com.witboost.provisioning.framework.service.validation.ValidationConfiguration;
import com.witboost.provisioning.s3.client.BucketManager;
import com.witboost.provisioning.s3.service.provision.StorageAreaProvisionService;
import com.witboost.provisioning.s3.service.validation.StorageAreaValidationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

class ConfigurationBeanTest {

    @InjectMocks
    private ConfigurationBean configurationBean;

    @Mock
    private BucketManager bucketManager;

    @Mock
    private StsClient stsClient;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testStorageAreaProvisionServiceBean() {
        StorageAreaProvisionService service = configurationBean.storageAreaProvisionService(stsClient);
        assertNotNull(service, "StorageAreaProvisionService bean should not be null");
    }

    @Test
    void testProvisionConfigurationBean() {
        StorageAreaProvisionService service = configurationBean.storageAreaProvisionService(stsClient);

        ProvisionConfiguration configuration = configurationBean.provisionConfiguration(service);

        assertNotNull(configuration, "ProvisionConfiguration bean should not be null");
        assertEquals(
                service,
                configuration.getStorageProvisionService(),
                "StorageProvisionService should match the injected bean");
    }

    @Test
    void testStorageAreaValidationServiceBean() {
        StorageAreaValidationService service = configurationBean.storageAreaValidationService(bucketManager);
    }

    @Test
    void testValidationConfigurationBean() {
        StorageAreaValidationService service = configurationBean.storageAreaValidationService(bucketManager);
        ValidationConfiguration validationConfigurationBean = new ConfigurationBean().validationConfiguration(service);

        assertEquals(service, validationConfigurationBean.getStorageValidationService());
        assertEquals(StorageAreaValidationService.class, service.getClass());
    }

    @Test
    void testGetS3ClientCaching() {
        Region region = Region.US_WEST_2;
        S3Client s3Client1 = configurationBean.getS3Client(region);
        S3Client s3Client2 = configurationBean.getS3Client(region);

        // Assert that the S3 client is cached
        assertNotNull(s3Client1, "S3Client should not be null");
        assertSame(s3Client1, s3Client2, "S3Client should be cached and return the same instance");
    }

    @Test
    void testGetKmsClientCaching() {
        Region region = Region.US_WEST_2;
        KmsClient kmsClient1 = configurationBean.getKmsClient(region);
        KmsClient kmsClient2 = configurationBean.getKmsClient(region);

        // Assert that the KMS client is cached
        assertNotNull(kmsClient1, "KmsClient should not be null");
        assertSame(kmsClient1, kmsClient2, "KmsClient should be cached and return the same instance");
    }
}
