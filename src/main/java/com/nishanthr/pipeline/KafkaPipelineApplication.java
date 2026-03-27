package com.nishanthr.pipeline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EnableCaching
@EnableJpaRepositories(basePackages = "com.nishanthr.pipeline.persistence")
@EntityScan(basePackages = "com.nishanthr.pipeline.model")
public class KafkaPipelineApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaPipelineApplication.class, args);
    }
}