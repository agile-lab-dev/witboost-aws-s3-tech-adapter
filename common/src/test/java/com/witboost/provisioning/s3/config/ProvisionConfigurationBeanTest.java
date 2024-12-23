package com.witboost.provisioning.s3.config;

import static org.junit.jupiter.api.Assertions.*;

import com.witboost.provisioning.s3.service.provision.StorageAreaProvisionService;
import org.junit.jupiter.api.Test;

class ProvisionConfigurationBeanTest {

    @Test
    void beanCreation() {
        var storageArea = new StorageAreaProvisionService();
        var bean = new ProvisionConfigurationBean().provisionConfiguration(storageArea);

        assertEquals(storageArea, bean.getStorageProvisionService());
    }
}
