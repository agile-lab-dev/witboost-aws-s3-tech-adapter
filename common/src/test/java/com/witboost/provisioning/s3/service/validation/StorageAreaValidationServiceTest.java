package com.witboost.provisioning.s3.service.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import com.witboost.provisioning.s3.model.S3Specific;
import io.vavr.control.Either;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class StorageAreaValidationServiceTest {

    @Test
    void validate_success() {
        OperationRequest<?, S3Specific> request = mock(OperationRequest.class);
        S3Specific validSpecific = new S3Specific();
        validSpecific.setMultipleVersion(false);
        validSpecific.setRegion("eu-west-1");
        validSpecific.setAccountId("accountId");

        var component = mock(com.witboost.provisioning.model.Component.class);
        when(component.getSpecific()).thenReturn(validSpecific);
        when(request.getComponent()).thenReturn(Optional.of(component));

        Either<FailedOperation, Void> result = validationService.validate(request, OperationType.PROVISION);

        assertTrue(result.isRight());
    }

    StorageAreaValidationService validationService = new StorageAreaValidationService();

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
                Collections.singletonList(new Problem("Invalid Specific type of fake-storage. Expected S3Specific.")));

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
