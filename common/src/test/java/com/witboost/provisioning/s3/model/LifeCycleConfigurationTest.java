package com.witboost.provisioning.s3.model;

import static org.junit.jupiter.api.Assertions.*;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LifeCycleConfigurationTest {

    private Validator validator;

    @BeforeEach
    void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Test
    void testValidConfiguration() {
        LifeCycleConfigurationPermanentlyDelete permanentlyDelete = new LifeCycleConfigurationPermanentlyDelete();
        permanentlyDelete.setDaysAfterBecomeNonCurrent(10);
        permanentlyDelete.setNumberOfVersionsToRetain(5);

        LifeCycleConfiguration config = new LifeCycleConfiguration();
        config.setPermanentlyDelete(permanentlyDelete);

        Set<ConstraintViolation<LifeCycleConfiguration>> violations = validator.validate(config);
        assertTrue(violations.isEmpty());
    }

    @Test
    void testGetterSetter() {
        LifeCycleConfigurationPermanentlyDelete permanentlyDelete = new LifeCycleConfigurationPermanentlyDelete();
        permanentlyDelete.setDaysAfterBecomeNonCurrent(15);
        permanentlyDelete.setNumberOfVersionsToRetain(7);

        LifeCycleConfiguration config = new LifeCycleConfiguration();
        config.setPermanentlyDelete(permanentlyDelete);

        assertEquals(15, config.getPermanentlyDelete().getDaysAfterBecomeNonCurrent());
        assertEquals(7, config.getPermanentlyDelete().getNumberOfVersionsToRetain());
    }

    @Test
    void testInvalidDaysAfterBecomeNonCurrent_BelowMin() {
        LifeCycleConfigurationPermanentlyDelete permanentlyDelete = new LifeCycleConfigurationPermanentlyDelete();
        permanentlyDelete.setDaysAfterBecomeNonCurrent(0);
        permanentlyDelete.setNumberOfVersionsToRetain(5);

        LifeCycleConfiguration config = new LifeCycleConfiguration();
        config.setPermanentlyDelete(permanentlyDelete);

        Set<ConstraintViolation<LifeCycleConfiguration>> violations = validator.validate(config);
        assertFalse(violations.isEmpty());
    }

    @Test
    void testInvalidNumberOfVersionsToRetain_BelowMin() {
        LifeCycleConfigurationPermanentlyDelete permanentlyDelete = new LifeCycleConfigurationPermanentlyDelete();
        permanentlyDelete.setDaysAfterBecomeNonCurrent(10);
        permanentlyDelete.setNumberOfVersionsToRetain(0);

        LifeCycleConfiguration config = new LifeCycleConfiguration();
        config.setPermanentlyDelete(permanentlyDelete);

        Set<ConstraintViolation<LifeCycleConfiguration>> violations = validator.validate(config);
        assertFalse(violations.isEmpty());
    }

    @Test
    void testInvalidNumberOfVersionsToRetain_AboveMax() {
        LifeCycleConfigurationPermanentlyDelete permanentlyDelete = new LifeCycleConfigurationPermanentlyDelete();
        permanentlyDelete.setDaysAfterBecomeNonCurrent(10);
        permanentlyDelete.setNumberOfVersionsToRetain(150);

        LifeCycleConfiguration config = new LifeCycleConfiguration();
        config.setPermanentlyDelete(permanentlyDelete);

        Set<ConstraintViolation<LifeCycleConfiguration>> violations = validator.validate(config);
        assertFalse(violations.isEmpty());
    }

    @Test
    void testValidBoundaryDaysAfterBecomeNonCurrent() {
        LifeCycleConfigurationPermanentlyDelete permanentlyDelete = new LifeCycleConfigurationPermanentlyDelete();
        permanentlyDelete.setDaysAfterBecomeNonCurrent(1);
        permanentlyDelete.setNumberOfVersionsToRetain(5);

        LifeCycleConfiguration config = new LifeCycleConfiguration();
        config.setPermanentlyDelete(permanentlyDelete);

        Set<ConstraintViolation<LifeCycleConfiguration>> violations = validator.validate(config);
        assertTrue(violations.isEmpty());
    }

    @Test
    void testValidBoundaryNumberOfVersionsToRetain() {
        LifeCycleConfigurationPermanentlyDelete permanentlyDelete = new LifeCycleConfigurationPermanentlyDelete();
        permanentlyDelete.setDaysAfterBecomeNonCurrent(10);
        permanentlyDelete.setNumberOfVersionsToRetain(100);

        LifeCycleConfiguration config = new LifeCycleConfiguration();
        config.setPermanentlyDelete(permanentlyDelete);

        Set<ConstraintViolation<LifeCycleConfiguration>> violations = validator.validate(config);
        assertTrue(violations.isEmpty());
    }

    @Test
    void testInvalidBoundaryDaysAfterBecomeNonCurrent() {
        LifeCycleConfigurationPermanentlyDelete permanentlyDelete = new LifeCycleConfigurationPermanentlyDelete();
        permanentlyDelete.setDaysAfterBecomeNonCurrent(-1);
        permanentlyDelete.setNumberOfVersionsToRetain(5);

        LifeCycleConfiguration config = new LifeCycleConfiguration();
        config.setPermanentlyDelete(permanentlyDelete);

        Set<ConstraintViolation<LifeCycleConfiguration>> violations = validator.validate(config);
        assertFalse(violations.isEmpty());
    }

    @Test
    void testInvalidBoundaryNumberOfVersionsToRetain() {
        LifeCycleConfigurationPermanentlyDelete permanentlyDelete = new LifeCycleConfigurationPermanentlyDelete();
        permanentlyDelete.setDaysAfterBecomeNonCurrent(10);
        permanentlyDelete.setNumberOfVersionsToRetain(101);

        LifeCycleConfiguration config = new LifeCycleConfiguration();
        config.setPermanentlyDelete(permanentlyDelete);

        Set<ConstraintViolation<LifeCycleConfiguration>> violations = validator.validate(config);
        assertFalse(violations.isEmpty());
    }
}
