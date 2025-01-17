package com.witboost.provisioning.s3.config;

import static org.junit.jupiter.api.Assertions.*;

import com.witboost.provisioning.framework.service.ProvisionConfiguration;
import com.witboost.provisioning.s3.client.BucketManager;
import com.witboost.provisioning.s3.service.provision.StorageAreaProvisionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

class ProvisionConfigurationBeanTest {

    @InjectMocks
    private ProvisionConfigurationBean provisionConfigurationBean;

    @Mock
    private BucketManager bucketManager;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testStorageAreaProvisionServiceBean() {
        StorageAreaProvisionService service = provisionConfigurationBean.storageAreaProvisionService(bucketManager);
        assertNotNull(service, "StorageAreaProvisionService bean should not be null");
    }

    @Test
    void testProvisionConfigurationBean() {
        StorageAreaProvisionService service = provisionConfigurationBean.storageAreaProvisionService(bucketManager);

        ProvisionConfiguration configuration = provisionConfigurationBean.provisionConfiguration(service);

        assertNotNull(configuration, "ProvisionConfiguration bean should not be null");
        assertEquals(
                service,
                configuration.getStorageProvisionService(),
                "StorageProvisionService should match the injected bean");
    }

    @Test
    void testGetS3ClientCaching() {
        Region region = Region.US_WEST_2;
        S3Client s3Client1 = provisionConfigurationBean.getS3Client(region);
        S3Client s3Client2 = provisionConfigurationBean.getS3Client(region);

        // Assert that the S3 client is cached
        assertNotNull(s3Client1, "S3Client should not be null");
        assertSame(s3Client1, s3Client2, "S3Client should be cached and return the same instance");
    }
}
