package com.msalimcengiz.saga.testsupport;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Real Kafka and Postgres for integration tests. Spring caches the context between test
 * classes, so each module starts the containers once.
 */
@TestConfiguration(proxyBeanMethods = false)
public class Infra {

    @Bean
    @ServiceConnection
    public KafkaContainer kafka() {
        return new KafkaContainer("apache/kafka:4.2.0");
    }

    @Bean
    @ServiceConnection
    public PostgreSQLContainer postgres() {
        return new PostgreSQLContainer("postgres:17-alpine");
    }
}
