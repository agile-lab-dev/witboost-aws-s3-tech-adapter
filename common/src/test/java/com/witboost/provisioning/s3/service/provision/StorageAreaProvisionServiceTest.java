package com.witboost.provisioning.s3.service.provision;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.witboost.provisioning.model.DataProduct;
import com.witboost.provisioning.model.OutputPort;
import com.witboost.provisioning.model.Specific;
import com.witboost.provisioning.model.StorageArea;
import com.witboost.provisioning.model.common.FailedOperation;
import com.witboost.provisioning.model.request.ProvisionOperationRequest;
import com.witboost.provisioning.model.status.ProvisionInfo;
import com.witboost.provisioning.s3.client.BucketManager;
import com.witboost.provisioning.s3.model.S3Specific;
import io.vavr.control.Either;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

class StorageAreaProvisionServiceTest {

    @Mock
    private Function<Region, S3Client> s3ClientProvider;

    @Mock
    private Function<Region, KmsClient> kmsClientProvider;

    @Mock
    private BucketManager bucketManager;

    @Mock
    private S3Client s3Client;

    @Mock
    private KmsClient kmsClient;

    @Mock
    private StsClient stsClient;

    @Mock
    private ProvisionOperationRequest<?, ? extends Specific> request;

    private StorageAreaProvisionService storageAreaProvisionService;

    private String bucketName = "domain-dataproduct-componentname-devfb80c";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        when(s3ClientProvider.apply(any(Region.class))).thenReturn(s3Client);
        when(kmsClientProvider.apply(any(Region.class))).thenReturn(kmsClient);

        storageAreaProvisionService =
                new StorageAreaProvisionService(s3ClientProvider, kmsClientProvider, stsClient, bucketManager);

        GetCallerIdentityResponse callerIdentityResponse = mock(GetCallerIdentityResponse.class);
        when(stsClient.getCallerIdentity()).thenReturn(callerIdentityResponse);
        when(callerIdentityResponse.account()).thenReturn("accountId");

        when(request.getComponent()).thenReturn(Optional.of(createStorageArea()));
        when(request.getDataProduct()).thenReturn(createDataProduct());
    }

    @Test
    void testProvision_success() {
        when(bucketManager.createOrUpdateBucket(
                        eq(s3Client), eq(kmsClient), eq(bucketName), any(S3Specific.class), anyString()))
                .thenReturn(Either.right(null));
        when(bucketManager.createFolder(eq(s3Client), eq(bucketName), anyString()))
                .thenReturn(Either.right(null));

        Either<FailedOperation, ProvisionInfo> result = storageAreaProvisionService.provision(request);

        assertTrue(result.isRight(), "Provision should succeed");
        verify(bucketManager)
                .createOrUpdateBucket(eq(s3Client), eq(kmsClient), eq(bucketName), any(S3Specific.class), anyString());
        verify(bucketManager).createFolder(eq(s3Client), eq(bucketName), anyString());
    }

    @Test
    void testProvision_success1() {

        when(s3ClientProvider.apply(any(Region.class))).thenReturn(s3Client);
        when(bucketManager.createOrUpdateBucket(
                        eq(s3Client), eq(kmsClient), eq(bucketName), any(S3Specific.class), anyString()))
                .thenReturn(Either.right(null));
        when(bucketManager.createFolder(eq(s3Client), eq(bucketName), anyString()))
                .thenReturn(Either.right(null));

        Either<FailedOperation, ProvisionInfo> result = storageAreaProvisionService.provision(request);

        assertTrue(result.isRight(), "Provision should succeed");
        verify(bucketManager)
                .createOrUpdateBucket(eq(s3Client), eq(kmsClient), eq(bucketName), any(S3Specific.class), anyString());
        verify(bucketManager).createFolder(eq(s3Client), eq(bucketName), anyString());

        var privateInfo =
                new ObjectMapper().convertValue(result.get().getPrivateInfo().get(), Map.class);
        var publicInfo =
                new ObjectMapper().convertValue(result.get().getPublicInfo().get(), Map.class);

        assertEquals(3, privateInfo.size());
        assertEquals(3, publicInfo.size());

        assertTrue(privateInfo.containsKey("location"));
        assertTrue(privateInfo.containsKey("bucket"));
        assertTrue(privateInfo.containsKey("folder"));
    }

    @Test
    void testProvision_WrongComponent() {
        when(request.getComponent()).thenReturn(Optional.empty());
        Either<FailedOperation, ProvisionInfo> result = storageAreaProvisionService.provision(request);

        assertTrue(result.isLeft(), "Provision should fail if component is empty");
        assertEquals(
                "Invalid operation request: Component is missing. Request: request",
                result.getLeft().message());
    }

    @Test
    void testProvision_ComponentIsNotAStorageArea() {
        when(request.getComponent()).thenReturn(Optional.of(new OutputPort<>()));
        Either<FailedOperation, ProvisionInfo> result = storageAreaProvisionService.provision(request);

        assertTrue(result.isLeft(), "Provision should fail if component is not a storage area");
        assertEquals(
                "Invalid component type. null is not a valid Storage Area",
                result.getLeft().message());
    }

    @Test
    void testProvision_wrongSpecific() {

        var component = createStorageArea();
        component.setSpecific(new Specific());
        when(request.getComponent()).thenReturn(Optional.of(component));
        Either<FailedOperation, ProvisionInfo> result = storageAreaProvisionService.provision(request);

        assertTrue(result.isLeft(), "Unprovision should fail if component specific is wrong");
        assertEquals(
                "Invalid Specific type of fake-storage. Expected S3Specific.",
                result.getLeft().message());
    }

    @Test
    void testProvision_bucketCreationFailure() {

        when(s3ClientProvider.apply(any(Region.class))).thenReturn(s3Client);
        when(bucketManager.createOrUpdateBucket(
                        eq(s3Client), eq(kmsClient), eq(bucketName), any(S3Specific.class), anyString()))
                .thenReturn(Either.left(new FailedOperation("Bucket creation failed", Collections.emptyList())));

        Either<FailedOperation, ProvisionInfo> result = storageAreaProvisionService.provision(request);

        assertTrue(result.isLeft(), "Provision should fail if bucket creation fails");
        assertEquals("Bucket creation failed", result.getLeft().message());
        verify(bucketManager)
                .createOrUpdateBucket(eq(s3Client), eq(kmsClient), eq(bucketName), any(S3Specific.class), anyString());
        verify(bucketManager, never()).createFolder(any(), any(), any());
    }

    @Test
    void testProvision_folderCreationFailure() {

        when(s3ClientProvider.apply(any(Region.class))).thenReturn(s3Client);
        when(bucketManager.createOrUpdateBucket(
                        eq(s3Client), eq(kmsClient), eq(bucketName), any(S3Specific.class), anyString()))
                .thenReturn(Either.right(null));
        when(bucketManager.createFolder(eq(s3Client), eq(bucketName), anyString()))
                .thenReturn(Either.left(new FailedOperation("Folder creation failed", Collections.emptyList())));

        Either<FailedOperation, ProvisionInfo> result = storageAreaProvisionService.provision(request);

        assertTrue(result.isLeft(), "Provision should fail if folder creation fails");
        assertEquals("Folder creation failed", result.getLeft().message());
        verify(bucketManager)
                .createOrUpdateBucket(eq(s3Client), eq(kmsClient), eq(bucketName), any(S3Specific.class), anyString());
        verify(bucketManager).createFolder(any(), any(), any());
    }

    @Test
    void testUnprovision_success() {
        when(s3ClientProvider.apply(any(Region.class))).thenReturn(s3Client);
        when(bucketManager.doesBucketExist(eq(s3Client), eq(bucketName))).thenReturn(Either.right(true));
        when(bucketManager.deleteObjectsWithPrefix(eq(s3Client), eq(bucketName), anyString()))
                .thenReturn(Either.right(null));

        Either<FailedOperation, ProvisionInfo> result = storageAreaProvisionService.unprovision(request);

        assertTrue(result.isRight(), "Unprovision should succeed");
    }

    @Test
    void testUnprovision_WrongComponent() {
        when(request.getComponent()).thenReturn(Optional.empty());
        Either<FailedOperation, ProvisionInfo> result = storageAreaProvisionService.unprovision(request);

        assertTrue(result.isLeft(), "Unprovision should fail if component is empty");
        assertEquals(
                "Invalid operation request: Component is missing. Request: request",
                result.getLeft().message());
    }

    @Test
    void testUnprovision_bucketDoesNotExist() {
        when(s3ClientProvider.apply(any(Region.class))).thenReturn(s3Client);
        when(bucketManager.doesBucketExist(eq(s3Client), eq(bucketName))).thenReturn(Either.right(false));

        Either<FailedOperation, ProvisionInfo> result = storageAreaProvisionService.unprovision(request);

        assertTrue(result.isRight(), "Unprovision should succeed if bucket does not exist");
        verify(bucketManager, never()).deleteObjectsWithPrefix(any(), any(), any());
    }

    private S3Specific createS3Specific() {
        S3Specific s3Specific = new S3Specific();
        s3Specific.setRegion("us-west-2");
        s3Specific.setMultipleVersion(false);
        return s3Specific;
    }

    private StorageArea createStorageArea() {
        StorageArea storage = new StorageArea<>();
        storage.setName("fake-storage");
        storage.setSpecific(createS3Specific());
        storage.setId("urn:dmb:cmp:domain:dp:0:componentname");
        return storage;
    }

    private DataProduct createDataProduct() {
        var dataProduct = new DataProduct<>();
        dataProduct.setDomain("domain");
        dataProduct.setName("dataproduct");
        dataProduct.setEnvironment("dev");
        return dataProduct;
    }
}
