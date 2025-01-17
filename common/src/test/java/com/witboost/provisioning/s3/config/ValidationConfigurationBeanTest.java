package com.witboost.provisioning.s3.config;

import static org.junit.jupiter.api.Assertions.*;

import com.witboost.provisioning.s3.service.validation.StorageAreaValidationService;
import org.junit.jupiter.api.Test;

class ValidationConfigurationBeanTest {

    @Test
    void beanCreation() {
        var storageArea = new StorageAreaValidationService();
        var validationConfigurationBean = new ValidationConfigurationBean().validationConfiguration(storageArea);
        var storageAreaValidationServiceBean = new ValidationConfigurationBean().storageAreaValidationService();

        assertEquals(storageArea, validationConfigurationBean.getStorageValidationService());
        assertEquals(StorageAreaValidationService.class, storageAreaValidationServiceBean.getClass());
    }
}
