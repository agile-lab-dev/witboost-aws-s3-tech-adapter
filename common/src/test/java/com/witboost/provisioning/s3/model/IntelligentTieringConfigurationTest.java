package com.witboost.provisioning.s3.model;

import static org.junit.jupiter.api.Assertions.*;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IntelligentTieringConfigurationTest {

    private Validator validator;

    @BeforeEach
    void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Test
    void testValidConfiguration() {
        IntelligentTieringConfiguration config = new IntelligentTieringConfiguration();
        config.setArchiveAccessTierEnabled(true);
        config.setDeepArchiveAccessTierEnabled(true);
        config.setArchiveAccessTierDays(120);
        config.setDeepArchiveAccessTierDays(200);

        Set<ConstraintViolation<IntelligentTieringConfiguration>> violations = validator.validate(config);
        assertTrue(violations.isEmpty(), "Expected no validation errors");
    }

    @Test
    void testGettersAndSetters() {
        IntelligentTieringConfiguration config = new IntelligentTieringConfiguration();
        config.setArchiveAccessTierEnabled(false);
        config.setDeepArchiveAccessTierEnabled(false);
        config.setArchiveAccessTierDays(100);
        config.setDeepArchiveAccessTierDays(300);

        assertFalse(config.getArchiveAccessTierEnabled());
        assertFalse(config.getDeepArchiveAccessTierEnabled());
        assertEquals(100, config.getArchiveAccessTierDays());
        assertEquals(300, config.getDeepArchiveAccessTierDays());
    }

    @Test
    void testEnabledCanBeNull() {
        IntelligentTieringConfiguration config = new IntelligentTieringConfiguration();
        config.setArchiveAccessTierEnabled(null);
        config.setDeepArchiveAccessTierEnabled(null);

        assertNull(config.getArchiveAccessTierEnabled());
        assertNull(config.getDeepArchiveAccessTierEnabled());
    }

    @Test
    void testArchiveAccessTierDays_AtLowerBound() {
        IntelligentTieringConfiguration config = new IntelligentTieringConfiguration();
        config.setArchiveAccessTierDays(90);
        config.setDeepArchiveAccessTierDays(185);

        Set<ConstraintViolation<IntelligentTieringConfiguration>> violations = validator.validate(config);
        assertTrue(violations.isEmpty(), "Expected no validation errors for archiveAccessTierDays = 90");
    }

    @Test
    void testArchiveAccessTierDays_AtUpperBound() {
        IntelligentTieringConfiguration config = new IntelligentTieringConfiguration();
        config.setArchiveAccessTierDays(730);
        config.setDeepArchiveAccessTierDays(185);

        Set<ConstraintViolation<IntelligentTieringConfiguration>> violations = validator.validate(config);
        assertTrue(violations.isEmpty(), "Expected no validation errors for archiveAccessTierDays = 730");
    }

    @Test
    void testDeepArchiveAccessTierDays_AtLowerBound() {
        IntelligentTieringConfiguration config = new IntelligentTieringConfiguration();
        config.setArchiveAccessTierDays(95);
        config.setDeepArchiveAccessTierDays(180);

        Set<ConstraintViolation<IntelligentTieringConfiguration>> violations = validator.validate(config);
        assertTrue(violations.isEmpty(), "Expected no validation errors for deepArchiveAccessTierDays = 180");
    }

    @Test
    void testDeepArchiveAccessTierDays_AtUpperBound() {
        IntelligentTieringConfiguration config = new IntelligentTieringConfiguration();
        config.setArchiveAccessTierDays(95);
        config.setDeepArchiveAccessTierDays(730);

        Set<ConstraintViolation<IntelligentTieringConfiguration>> violations = validator.validate(config);
        assertTrue(violations.isEmpty(), "Expected no validation errors for deepArchiveAccessTierDays = 730");
    }

    @Test
    void testInvalidArchiveAccessTierDays_BelowMin() {
        IntelligentTieringConfiguration config = new IntelligentTieringConfiguration();
        config.setArchiveAccessTierEnabled(true);
        config.setArchiveAccessTierDays(80);

        Set<ConstraintViolation<IntelligentTieringConfiguration>> violations = validator.validate(config);
        assertFalse(violations.isEmpty(), "Expected validation error for archiveAccessTierDays < 90");
    }

    @Test
    void testInvalidArchiveAccessTierDays_AboveMax() {
        IntelligentTieringConfiguration config = new IntelligentTieringConfiguration();
        config.setArchiveAccessTierEnabled(true);
        config.setArchiveAccessTierDays(750);

        Set<ConstraintViolation<IntelligentTieringConfiguration>> violations = validator.validate(config);
        assertFalse(violations.isEmpty(), "Expected validation error for archiveAccessTierDays > 730");
    }

    @Test
    void testInvalidDeepArchiveAccessTierDays_BelowMin() {
        IntelligentTieringConfiguration config = new IntelligentTieringConfiguration();
        config.setDeepArchiveAccessTierEnabled(true);
        config.setDeepArchiveAccessTierDays(150);

        Set<ConstraintViolation<IntelligentTieringConfiguration>> violations = validator.validate(config);
        assertFalse(violations.isEmpty(), "Expected validation error for deepArchiveAccessTierDays < 180");
    }

    @Test
    void testInvalidDeepArchiveAccessTierDays_AboveMax() {
        IntelligentTieringConfiguration config = new IntelligentTieringConfiguration();
        config.setDeepArchiveAccessTierEnabled(true);
        config.setDeepArchiveAccessTierDays(800);

        Set<ConstraintViolation<IntelligentTieringConfiguration>> violations = validator.validate(config);
        assertFalse(violations.isEmpty(), "Expected validation error for deepArchiveAccessTierDays > 730");
    }
}
