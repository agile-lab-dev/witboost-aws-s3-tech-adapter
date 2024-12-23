package com.witboost.provisioning.s3.service.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.witboost.provisioning.model.DataProduct;
import com.witboost.provisioning.model.OperationType;
import com.witboost.provisioning.model.OutputPort;
import com.witboost.provisioning.model.common.FailedOperation;
import com.witboost.provisioning.model.common.Problem;
import com.witboost.provisioning.model.request.ProvisionOperationRequest;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/*
 * TODO Review these tests after you have implemented the tech adapter logic
 */
class StorageAreaValidationServiceTest {

    FailedOperation expectedError = new FailedOperation(
            "Validation for the operation request not supported",
            Collections.singletonList(new Problem(
                    "This adapter doesn't support validation for the received request",
                    Set.of(
                            "Ensure that the adapter is registered correctly for this type of request and that the ValidationConfiguration is set up to support the requested component",
                            "Please try again. If the problem persists, contact the platform team."))));

    @Test
    void validate() {
        var validationService = new StorageAreaValidationService();

        var actual = validationService.validate(
                new ProvisionOperationRequest<>(new DataProduct<>(), new OutputPort<>(), false, Optional.empty()),
                OperationType.VALIDATE);
        assertTrue(actual.isLeft());
        assertEquals(expectedError, actual.getLeft());
    }
}
