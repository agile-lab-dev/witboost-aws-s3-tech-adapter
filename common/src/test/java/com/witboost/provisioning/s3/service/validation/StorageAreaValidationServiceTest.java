package com.witboost.provisioning.s3.service.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.witboost.provisioning.model.DataProduct;
import com.witboost.provisioning.model.OperationType;
import com.witboost.provisioning.model.Specific;
import com.witboost.provisioning.model.StorageArea;
import com.witboost.provisioning.model.common.FailedOperation;
import com.witboost.provisioning.model.common.Problem;
import com.witboost.provisioning.model.request.OperationRequest;
import com.witboost.provisioning.model.request.ProvisionOperationRequest;
import com.witboost.provisioning.s3.client.BucketManager;
import com.witboost.provisioning.s3.model.S3Specific;
import io.vavr.control.Either;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

class StorageAreaValidationServiceTest {

    @Mock
    private Function<Region, S3Client> s3ClientProvider;

    @Mock
    private BucketManager bucketManager;

    @Mock
    private S3Client s3Client;

    @Mock
    private ProvisionOperationRequest<?, ? extends Specific> request;

    @InjectMocks
    private StorageAreaValidationService validationService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(s3ClientProvider.apply(any(Region.class))).thenReturn(s3Client);
    }

    @Test
    void validate_success_sameRegion() {
        OperationRequest<?, S3Specific> request = mock(OperationRequest.class);
        S3Specific validSpecific = new S3Specific();
        validSpecific.setMultipleVersion(false);
        validSpecific.setRegion("eu-west-1");

        var component = mock(com.witboost.provisioning.model.Component.class);
        when(component.getSpecific()).thenReturn(validSpecific);
        when(request.getComponent()).thenReturn(Optional.of(component));
        when(component.getId()).thenReturn("urn:dmb:cmp:finance:reporting:0:raw-storage-area");
        DataProduct dp = new DataProduct<>();
        dp.setName("dp");
        when(request.getDataProduct()).thenReturn(dp);

        when(bucketManager.doesBucketExist(any(S3Client.class), anyString())).thenReturn(Either.right(true));
        when(bucketManager.getBucketRegion(any(S3Client.class), anyString())).thenReturn(Either.right("eu-west-1"));

        Either<FailedOperation, Void> result = validationService.validate(request, OperationType.PROVISION);

        assertTrue(result.isRight());
    }

    @Test
    void validate_success_bucketNotFound() {
        OperationRequest<?, S3Specific> request = mock(OperationRequest.class);
        S3Specific validSpecific = new S3Specific();
        validSpecific.setMultipleVersion(false);
        validSpecific.setRegion("eu-west-1");

        var component = mock(com.witboost.provisioning.model.Component.class);
        when(component.getSpecific()).thenReturn(validSpecific);
        when(request.getComponent()).thenReturn(Optional.of(component));
        when(component.getId()).thenReturn("urn:dmb:cmp:finance:reporting:0:raw-storage-area");
        DataProduct dp = new DataProduct<>();
        dp.setName("dp");
        when(request.getDataProduct()).thenReturn(dp);

        when(bucketManager.doesBucketExist(any(S3Client.class), anyString())).thenReturn(Either.right(false));

        Either<FailedOperation, Void> result = validationService.validate(request, OperationType.PROVISION);

        assertTrue(result.isRight());
    }

    @Test
    void validate_bucketExistsInDifferentRegion() {
        OperationRequest<?, S3Specific> request = mock(OperationRequest.class);
        S3Specific validSpecific = new S3Specific();
        validSpecific.setMultipleVersion(false);
        validSpecific.setRegion("eu-west-1");

        var component = mock(com.witboost.provisioning.model.Component.class);
        when(component.getSpecific()).thenReturn(validSpecific);
        when(request.getComponent()).thenReturn(Optional.of(component));
        DataProduct dp = new DataProduct<>();
        dp.setName("dp");
        when(request.getDataProduct()).thenReturn(dp);
        when(component.getId()).thenReturn("urn:dmb:cmp:finance:reporting:0:raw-storage-area");

        when(bucketManager.doesBucketExist(any(S3Client.class), anyString())).thenReturn(Either.right(true));
        when(bucketManager.getBucketRegion(any(S3Client.class), anyString())).thenReturn(Either.right("eu-central-1"));

        Either<FailedOperation, Void> result = validationService.validate(request, OperationType.PROVISION);

        assertTrue(result.isLeft());
        String error =
                "[Bucket: null-dp-raw-storage-area-null5842d] Error: The bucket is located in a different region from eu-west-1. Current region of the bucket: eu-central-1.";
        FailedOperation expectedError = new FailedOperation(error, List.of(new Problem(error)));

        assertEquals(expectedError, result.getLeft());
    }

    @Test
    void validate_emptyComponent() {

        StorageArea<Specific> storage = new StorageArea<>();
        storage.setName("fake-storage");
        var actual = validationService.validate(
                new ProvisionOperationRequest<>(new DataProduct<>(), storage, false, Optional.empty()),
                OperationType.VALIDATE);
        assertTrue(actual.isLeft());
        FailedOperation expectedError = new FailedOperation(
                "Invalid Specific type of fake-storage. Expected S3Specific.",
                List.of(new Problem("Invalid Specific type of fake-storage. Expected S3Specific.")));

        assertEquals(expectedError, actual.getLeft());
    }

    @Test
    void validate_failure_ComponentIsEmpty() {
        OperationRequest<?, S3Specific> request = mock(OperationRequest.class);
        when(request.getComponent()).thenReturn(Optional.empty());

        Either<FailedOperation, Void> result = validationService.validate(request, OperationType.PROVISION);

        assertTrue(result.isLeft());
        FailedOperation error = result.getLeft();
        assertEquals("Invalid operation request: Component is missing. Request: " + request, error.message());
    }

    @Test
    void validate_failure_SpecificTypeIsInvalid() {
        OperationRequest<?, Specific> request = mock(OperationRequest.class);
        var invalidSpecific = mock(Specific.class);

        var component = mock(StorageArea.class);
        when(component.getSpecific()).thenReturn(invalidSpecific);
        when(component.getName()).thenReturn("TestComponent");
        when(request.getComponent()).thenReturn(Optional.of(component));

        Either<FailedOperation, Void> result = validationService.validate(request, OperationType.PROVISION);

        assertTrue(result.isLeft());
        FailedOperation error = result.getLeft();
        assertEquals("Invalid Specific type of TestComponent. Expected S3Specific.", error.message());
    }
}
