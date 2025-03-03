package com.witboost.provisioning.s3.service.validation;

import com.witboost.provisioning.framework.service.validation.ComponentValidationService;
import com.witboost.provisioning.model.OperationType;
import com.witboost.provisioning.model.Specific;
import com.witboost.provisioning.model.common.FailedOperation;
import com.witboost.provisioning.model.common.Problem;
import com.witboost.provisioning.model.request.OperationRequest;
import com.witboost.provisioning.s3.client.BucketManager;
import com.witboost.provisioning.s3.model.S3Specific;
import com.witboost.provisioning.s3.utils.S3Utils;
import io.vavr.control.Either;
import jakarta.validation.Valid;
import java.util.List;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Component
@Validated
public class StorageAreaValidationService implements ComponentValidationService {

    private final Logger logger = LoggerFactory.getLogger(StorageAreaValidationService.class);

    private final Function<Region, S3Client> s3ClientProvider;
    private final BucketManager bucketManager;

    public StorageAreaValidationService(Function<Region, S3Client> s3ClientProvider, BucketManager bucketManager) {
        this.s3ClientProvider = s3ClientProvider;
        this.bucketManager = bucketManager;
    }

    @Override
    public Either<FailedOperation, Void> validate(
            @Valid OperationRequest<?, ? extends Specific> operationRequest, OperationType operationType) {

        var component = operationRequest.getComponent();
        if (component.isEmpty()) {
            String error =
                    String.format("Invalid operation request: Component is missing. Request: %s", operationRequest);
            logger.error(error);
            return Either.left(new FailedOperation(error, List.of(new Problem(error))));
        }

        var componentSpecific = component.get().getSpecific();

        if (!(componentSpecific instanceof @Valid S3Specific)) {
            String error = String.format(
                    "Invalid Specific type of %s. Expected S3Specific.",
                    component.get().getName());
            logger.error(error);
            return Either.left(new FailedOperation(error, List.of(new Problem(error))));
        }

        Region region = Region.of(((S3Specific) componentSpecific).getRegion());
        S3Client s3Client = s3ClientProvider.apply(region);

        String bucketName = S3Utils.computeBucketName(operationRequest.getDataProduct());

        Either<FailedOperation, Boolean> bucketExists = bucketManager.doesBucketExist(s3Client, bucketName);
        if (bucketExists.isLeft()) return Either.left(bucketExists.getLeft());

        // Bucket already exists
        if (bucketExists.get()) {
            Either<FailedOperation, String> existingRegion = bucketManager.getBucketRegion(s3Client, bucketName);
            if (existingRegion.isLeft()) return Either.left(existingRegion.getLeft());

            if (!region.id().equals(existingRegion.get())) {
                String error = String.format(
                        "[Bucket: %s] Error: The bucket is located in a different region from %s. Current region of the bucket: %s.",
                        bucketName, region, existingRegion.get());
                logger.error(error);
                return Either.left(new FailedOperation(error, List.of(new Problem(error))));
            }
        }

        return Either.right(null);
    }
}
