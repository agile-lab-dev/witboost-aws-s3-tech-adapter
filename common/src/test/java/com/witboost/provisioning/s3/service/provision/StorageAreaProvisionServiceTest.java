package com.witboost.provisioning.s3.service.provision;

import static org.junit.jupiter.api.Assertions.*;

import com.witboost.provisioning.model.DataProduct;
import com.witboost.provisioning.model.Specific;
import com.witboost.provisioning.model.StorageArea;
import com.witboost.provisioning.model.common.FailedOperation;
import com.witboost.provisioning.model.common.Problem;
import com.witboost.provisioning.model.request.AccessControlOperationRequest;
import com.witboost.provisioning.model.request.ProvisionOperationRequest;
import com.witboost.provisioning.model.request.ReverseProvisionOperationRequest;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/*
 * TODO Review these tests after you have implemented the tech adapter logic
 */
class StorageAreaProvisionServiceTest {

    @Test
    void provisionUnimplemented() {
        var provisionService = new StorageAreaProvisionService();
        var expectedError = new FailedOperation(
                "Provision for the operation request is not supported",
                Collections.singletonList(new Problem(
                        "This adapter doesn't support provisioning for the received request",
                        Set.of(
                                "Ensure that the adapter is registered correctly for this type of request and that the ProvisionConfiguration is set up to support the requested component",
                                "Please try again. If the problem persists, contact the platform team."))));
        var actual = provisionService.provision(
                new ProvisionOperationRequest<>(new DataProduct<>(), new StorageArea<>(), false, Optional.empty()));
        assertTrue(actual.isLeft());
        assertEquals(expectedError, actual.getLeft());
    }

    @Test
    void unprovisionUnimplemented() {
        var provisionService = new StorageAreaProvisionService();
        var expectedError = new FailedOperation(
                "Unprovision for the operation request is not supported",
                Collections.singletonList(new Problem(
                        "This adapter doesn't support unprovisioning for the received request",
                        Set.of(
                                "Ensure that the adapter is registered correctly for this type of request and that the ProvisionConfiguration is set up to support the requested component",
                                "Please try again. If the problem persists, contact the platform team."))));
        var actual = provisionService.unprovision(
                new ProvisionOperationRequest<>(new DataProduct<>(), new StorageArea<>(), false, Optional.empty()));
        assertTrue(actual.isLeft());
        assertEquals(expectedError, actual.getLeft());
    }

    @Test
    void updateAclUnimplemented() {
        var provisionService = new StorageAreaProvisionService();
        var expectedError = new FailedOperation(
                "Access control lists update for the operation request is not supported",
                Collections.singletonList(new Problem(
                        "This adapter doesn't support updating access control lists for the received request",
                        Set.of(
                                "Ensure that the adapter is registered correctly for this type of request and that the ProvisionConfiguration is set up to support the requested component",
                                "Please try again. If the problem persists, contact the platform team."))));
        var actual = provisionService.updateAcl(
                new AccessControlOperationRequest<>(new DataProduct<>(), Optional.of(new StorageArea<>()), Set.of()));
        assertTrue(actual.isLeft());
        assertEquals(expectedError, actual.getLeft());
    }

    @Test
    void reverseProvisionUnimplemented() {
        var provisionService = new StorageAreaProvisionService();
        var expectedError = new FailedOperation(
                "Reverse provisioning for the operation request is not supported",
                Collections.singletonList(new Problem(
                        "This adapter doesn't support reverse provisioning for the received request",
                        Set.of(
                                "Ensure that the adapter is registered correctly for this type of request and that the ProvisionConfiguration is set up to support the requested component",
                                "Please try again. If the problem persists, contact the platform team."))));
        var actual = provisionService.reverseProvision(
                new ReverseProvisionOperationRequest<>("useCaseTemplateId", "environment", new Specific(), null));
        assertTrue(actual.isLeft());
        assertEquals(expectedError, actual.getLeft());
    }
}
