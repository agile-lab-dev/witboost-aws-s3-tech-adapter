package com.witboost.provisioning.s3.config;

import static org.junit.jupiter.api.Assertions.*;

import com.witboost.provisioning.model.StorageArea;
import com.witboost.provisioning.s3.model.S3Specific;
import io.vavr.control.Option;
import org.junit.jupiter.api.Test;

class ClassProviderBeanTest {

    ClassProviderBean classProviderBean = new ClassProviderBean();

    @Test
    void defaultSpecificProvider() {
        var specificProvider = classProviderBean.specificClassProvider();

        assertEquals(Option.of(S3Specific.class), specificProvider.get("a-urn"));
    }

    @Test
    void defaultComponentProvider() {
        var componentProvider = classProviderBean.componentClassProvider();

        assertEquals(Option.of(StorageArea.class), componentProvider.get("whatever"));
    }
}
