package com.msalimcengiz.saga.inbox;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Deduplicates deliveries. Kafka commits the offset after the listener returns; if the
 * process dies after the database commit but before the offset commit, the record is
 * delivered again. Recording the message id in the same transaction as the side effects
 * turns that redelivery into a no-op.
 */
@Component
public class Inbox {

    private final JdbcClient jdbc;
    private final MeterRegistry meters;

    public Inbox(JdbcClient jdbc, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.meters = meters;
    }

    /** Returns true the first time a message id is seen, false for duplicates. */
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean firstDelivery(UUID messageId, String topic) {
        boolean first = jdbc.sql("INSERT INTO inbox (message_id, topic) VALUES (?, ?) ON CONFLICT DO NOTHING")
            .params(messageId, topic)
            .update() == 1;
        if (!first) {
            meters.counter("saga.inbox.duplicates", "topic", topic).increment();
        }
        return first;
    }
}
