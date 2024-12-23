package com.witboost.provisioning.s3.service.validation;

import com.witboost.provisioning.framework.service.validation.ComponentValidationService;
import com.witboost.provisioning.model.OperationType;
import com.witboost.provisioning.model.Specific;
import com.witboost.provisioning.model.common.FailedOperation;
import com.witboost.provisioning.model.request.OperationRequest;
import io.vavr.control.Either;
import jakarta.validation.Valid;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@Component
@Validated
public class StorageAreaValidationService implements ComponentValidationService {
    @Override
    public Either<FailedOperation, Void> validate(
            @Valid OperationRequest<?, ? extends Specific> operationRequest, OperationType operationType) {
        // TODO Remember to remove the super call and implement the validation for the storage area.
        //  If your tech adapter doesn't support storage areas, remove this class and its configuration
        //  on the ValidationConfigurationBean
        return ComponentValidationService.super.validate(operationRequest, operationType);
    }
}
