package com.witboost.provisioning.s3.service.provision;

import com.witboost.provisioning.framework.service.ProvisionService;
import com.witboost.provisioning.model.Specific;
import com.witboost.provisioning.model.common.FailedOperation;
import com.witboost.provisioning.model.request.ProvisionOperationRequest;
import com.witboost.provisioning.model.status.ProvisionInfo;
import io.vavr.control.Either;
import org.springframework.stereotype.Component;

@Component
public class StorageAreaProvisionService implements ProvisionService {
    @Override
    public Either<FailedOperation, ProvisionInfo> provision(
            ProvisionOperationRequest<?, ? extends Specific> operationRequest) {
        // TODO Remember to remove the super call and implement the provision for the storage area.
        return ProvisionService.super.provision(operationRequest);
    }

    @Override
    public Either<FailedOperation, ProvisionInfo> unprovision(
            ProvisionOperationRequest<?, ? extends Specific> operationRequest) {
        // TODO Remember to remove the super call and implement the unprovision for the storage area.
        return ProvisionService.super.unprovision(operationRequest);
    }
}
