package com.witboost.provisioning.s3.service.validation;

import com.witboost.provisioning.framework.service.validation.ComponentValidationService;
import com.witboost.provisioning.model.OperationType;
import com.witboost.provisioning.model.Specific;
import com.witboost.provisioning.model.common.FailedOperation;
import com.witboost.provisioning.model.common.Problem;
import com.witboost.provisioning.model.request.OperationRequest;
import com.witboost.provisioning.s3.model.S3Specific;
import io.vavr.control.Either;
import jakarta.validation.Valid;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@Component
@Validated
public class StorageAreaValidationService implements ComponentValidationService {

    private final Logger logger = LoggerFactory.getLogger(StorageAreaValidationService.class);

    @Override
    public Either<FailedOperation, Void> validate(
            @Valid OperationRequest<?, ? extends Specific> operationRequest, OperationType operationType) {

        var component = operationRequest.getComponent();
        if (component.isEmpty()) {
            String error =
                    String.format("Invalid operation request: Component is missing. Request: %s", operationRequest);
            logger.error(error);
            return Either.left(new FailedOperation(error, Collections.singletonList(new Problem(error))));
        }

        var componentSpecific = component.get().getSpecific();

        if (componentSpecific instanceof @Valid S3Specific) {
            return Either.right(null);
        } else {
            String error = String.format(
                    "Invalid Specific type of %s. Expected S3Specific.",
                    component.get().getName());
            logger.error(error);
            return Either.left(new FailedOperation(error, Collections.singletonList(new Problem(error))));
        }
    }
}
