package com.msalimcengiz.saga.payment;

import static org.assertj.core.api.Assertions.assertThat;

import com.msalimcengiz.saga.messages.Message.ChargePayment;
import com.msalimcengiz.saga.messages.MessageCodec;
import com.msalimcengiz.saga.messages.Topics;
import com.msalimcengiz.saga.testsupport.Infra;
import com.msalimcengiz.saga.testsupport.KafkaProbe;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/** Half of all attempts fail after the order was claimed; retries must still charge each order exactly once. */
@SpringBootTest(properties = {"saga.outbox.poll-interval-ms=50", "saga.fault-injection.rate=0.5", "saga.kafka.max-retries=20"})
@Import(Infra.class)
class TransientFailuresIT {

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired ConsumerFactory<String, String> consumers;
    @Autowired MessageCodec codec;
    @Autowired JdbcClient jdbc;

    @Test
    void everyOrderIsChargedExactlyOnce() {
        try (var probe = new KafkaProbe(kafka, consumers, codec, Topics.PAYMENT_EVENTS, Topics.deadLetter(Topics.PAYMENT_COMMANDS))) {
            Set<UUID> orders = new HashSet<>();
            for (int i = 0; i < 30; i++) {
                var id = UUID.randomUUID();
                orders.add(id);
                probe.send(new ChargePayment(id, "c020", new BigDecimal("10.00")));
            }

            probe.await(Topics.PAYMENT_EVENTS, r -> orders.contains(UUID.fromString(r.key())), 30, Duration.ofSeconds(60));

            assertThat(jdbc.sql("SELECT balance FROM accounts WHERE customer_id = 'c020'").query(BigDecimal.class).single())
                .isEqualByComparingTo("4700.00");
            assertThat(jdbc.sql("SELECT count(*) FROM payments WHERE customer_id = 'c020' AND status = 'CHARGED'")
                .query(Long.class).single()).isEqualTo(30);
            assertThat(probe.collect(Topics.deadLetter(Topics.PAYMENT_COMMANDS), r -> true, Duration.ofSeconds(1))).isEmpty();
        }
    }
}
