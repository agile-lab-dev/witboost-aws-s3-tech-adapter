package com.witboost.provisioning.s3.config;

import static org.junit.jupiter.api.Assertions.*;

import com.witboost.provisioning.s3.service.validation.StorageAreaValidationService;
import org.junit.jupiter.api.Test;

class ValidationConfigurationBeanTest {

    @Test
    void beanCreation() {
        var storageArea = new StorageAreaValidationService();
        var bean = new ValidationConfigurationBean().validationConfiguration(storageArea);

        assertEquals(storageArea, bean.getStorageValidationService());
    }
}
