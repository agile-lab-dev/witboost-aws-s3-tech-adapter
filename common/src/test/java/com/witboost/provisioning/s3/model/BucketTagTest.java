package com.witboost.provisioning.s3.model;

import static org.junit.jupiter.api.Assertions.*;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BucketTagTest {

    private Validator validator;

    @BeforeEach
    void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Test
    void testValidBucketTag() {
        BucketTag tag = new BucketTag();
        tag.setKey("Environment");
        tag.setValue("Production");

        Set<ConstraintViolation<BucketTag>> violations = validator.validate(tag);
        assertTrue(violations.isEmpty());
    }

    @Test
    void testInvalidBucketTag_BlankKey() {
        BucketTag tag = new BucketTag();
        tag.setKey("");
        tag.setValue("Production");

        Set<ConstraintViolation<BucketTag>> violations = validator.validate(tag);
        assertFalse(violations.isEmpty());
        assertEquals(1, violations.size());
    }

    @Test
    void testInvalidBucketTag_NullKey() {
        BucketTag tag = new BucketTag();
        tag.setKey(null);
        tag.setValue("Production");

        Set<ConstraintViolation<BucketTag>> violations = validator.validate(tag);
        assertFalse(violations.isEmpty());
        assertEquals(1, violations.size());
    }

    @Test
    void testInvalidBucketTag_BlankValue() {
        BucketTag tag = new BucketTag();
        tag.setKey("Environment");
        tag.setValue("");

        Set<ConstraintViolation<BucketTag>> violations = validator.validate(tag);
        assertFalse(violations.isEmpty());
        assertEquals(1, violations.size());
    }

    @Test
    void testInvalidBucketTag_NullValue() {
        BucketTag tag = new BucketTag();
        tag.setKey("Environment");
        tag.setValue(null);

        Set<ConstraintViolation<BucketTag>> violations = validator.validate(tag);
        assertFalse(violations.isEmpty());
        assertEquals(1, violations.size());
    }
}
