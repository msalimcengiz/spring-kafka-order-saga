package com.msalimcengiz.saga.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Publishes outbox rows to Kafka.
 *
 * <ul>
 *   <li>A transaction-scoped advisory lock makes one instance the publisher at a time,
 *       so rows for the same order are never sent out of order by two instances.
 *   <li>Rows are marked published only after Kafka acknowledged every send (acks=all,
 *       idempotent producer). If anything fails the transaction rolls back and the batch
 *       is sent again: delivery is at-least-once and consumers drop duplicates by message id.
 * </ul>
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    // Any constant works; it only has to be the same for all instances of one service.
    private static final long LOCK_KEY = 0x0B0C_5A6AL;

    private record Row(long id, String topic, String key, String payload) {}

    private final JdbcClient jdbc;
    private final TransactionTemplate tx;
    private final KafkaTemplate<String, String> kafka;
    private final int batchSize;
    private final Counter published;
    private final AtomicLong backlog = new AtomicLong();

    public OutboxRelay(
        JdbcClient jdbc,
        TransactionTemplate tx,
        KafkaTemplate<String, String> kafka,
        MeterRegistry meters,
        @Value("${saga.outbox.batch-size:500}") int batchSize
    ) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.kafka = kafka;
        this.batchSize = batchSize;
        this.published = meters.counter("saga.outbox.published");
        Gauge.builder("saga.outbox.backlog", backlog, AtomicLong::get)
            .description("Outbox rows not yet published")
            .register(meters);
    }

    @Scheduled(fixedDelayString = "${saga.outbox.poll-interval-ms:100}")
    public void publishPending() {
        try {
            Integer sent;
            do {
                sent = tx.execute(status -> publishBatch());
            } while (sent != null && sent == batchSize);
        } catch (Exception e) {
            log.warn("outbox publish failed; will retry: {}", e.toString());
        }
    }

    private int publishBatch() {
        Boolean leader = jdbc.sql("SELECT pg_try_advisory_xact_lock(?)").param(LOCK_KEY).query(Boolean.class).single();
        if (!Boolean.TRUE.equals(leader)) {
            return 0;
        }
        List<Row> rows = jdbc.sql("""
                SELECT id, topic, msg_key, payload FROM outbox
                WHERE published_at IS NULL ORDER BY id LIMIT ?""")
            .param(batchSize)
            .query((rs, n) -> new Row(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4)))
            .list();
        if (rows.isEmpty()) {
            backlog.set(0);
            return 0;
        }

        List<CompletableFuture<?>> sends = new ArrayList<>(rows.size());
        for (Row r : rows) {
            sends.add(kafka.send(r.topic(), r.key(), r.payload()));
        }
        try {
            CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Kafka did not acknowledge the batch", e);
        }

        // Mark exactly the rows that were sent. "id BETWEEN first AND last" would be wrong:
        // a transaction that took a lower id from the sequence but committed after the
        // SELECT would be marked published without ever being sent.
        Long[] ids = rows.stream().map(Row::id).toArray(Long[]::new);
        jdbc.sql("UPDATE outbox SET published_at = now() WHERE id = ANY(?)").param(ids).update();
        published.increment(rows.size());
        backlog.set(jdbc.sql("SELECT count(*) FROM outbox WHERE published_at IS NULL").query(Long.class).single());
        return rows.size();
    }

    /** Published rows are only needed for debugging; keep a day of them. */
    @Scheduled(fixedDelay = 60_000)
    public void cleanup() {
        jdbc.sql("DELETE FROM outbox WHERE published_at < now() - interval '1 day'").update();
        jdbc.sql("DELETE FROM inbox WHERE received_at < now() - interval '7 days'").update();
    }
}
