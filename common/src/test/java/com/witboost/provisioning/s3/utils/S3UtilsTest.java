package com.witboost.provisioning.s3.utils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.witboost.provisioning.model.Component;
import com.witboost.provisioning.model.DataProduct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class S3UtilsTest {

    private DataProduct<?> dataProduct;
    private Component<?> component;

    @BeforeEach
    void setUp() {
        dataProduct = mock(DataProduct.class);
        component = mock(Component.class);
    }

    @Test
    void testComputeBucketName_NormalCase() {
        when(dataProduct.getDomain()).thenReturn("finance");
        when(dataProduct.getName()).thenReturn("reporting");
        when(dataProduct.getEnvironment()).thenReturn("prod");
        when(component.getId()).thenReturn("urn:dmb:cmp:finance:reporting:0:raw-storage-area");

        String bucketName = S3Utils.computeBucketName(dataProduct, component);

        assertTrue(bucketName.startsWith("finance-reporting-raw-storage-area-prod"));
    }

    @Test
    void testComputeBucketName_WithSpaces() {
        when(dataProduct.getDomain()).thenReturn("  Sales ");
        when(dataProduct.getName()).thenReturn("Analytics ");
        when(dataProduct.getEnvironment()).thenReturn(" Dev ");
        when(component.getId()).thenReturn("urn:dmb:cmp:finance:reporting:0:raw-storage-area");

        String bucketName = S3Utils.computeBucketName(dataProduct, component);

        assert (bucketName.startsWith("sales-analytics-raw-storage-area-dev"));
    }

    @Test
    void testComputeBucketName_LongName() {
        when(dataProduct.getDomain()).thenReturn("verylongdomainnameexceedinglimits");
        when(dataProduct.getName()).thenReturn("verylongdatanameexceedinglimits");
        when(dataProduct.getEnvironment()).thenReturn("prod");
        when(component.getId()).thenReturn("urn:dmb:cmp:finance:reporting:0:raw-storage-area");

        String bucketName = S3Utils.computeBucketName(dataProduct, component);

        assertEquals(63, bucketName.length());
    }

    @Test
    void testSHA256() {

        assertEquals(
                "f44c106be492f4b0f947dcbd7380234f2f124f310c46e6c7a67ea8dc8255df38",
                S3Utils.sha256("finance-cashflow-dev"));
        assertEquals(
                "cfe5d9cd9e20a4f0ea718267b0bd1b21871f82b8edda79f8688ad50f22c963c8", S3Utils.sha256("hr-salaries-qa"));
        assertEquals(
                "02a0ae196d8e0a79363418df5bd581a6f5f7e43e4a8030dcd644e6ce07c5d1a2",
                S3Utils.sha256("sales-products-production"));
    }
}
