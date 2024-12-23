package com.witboost.provisioning.s3;

import com.witboost.provisioning.JavaTechAdapterFramework;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/** This is the Main class. */
@SpringBootApplication(scanBasePackageClasses = {JavaTechAdapterFramework.class, Main.class})
@ConfigurationPropertiesScan(basePackageClasses = {JavaTechAdapterFramework.class, Main.class})
public class Main {

    /** This is the main method which acts as the entry point inside the application. */
    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }
}
