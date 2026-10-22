package com.msalimcengiz.saga.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import com.msalimcengiz.saga.messages.Message;
import com.msalimcengiz.saga.messages.Message.ReserveStock;
import com.msalimcengiz.saga.messages.Message.StockRejected;
import com.msalimcengiz.saga.messages.Message.StockReserved;
import com.msalimcengiz.saga.messages.MessageCodec;
import com.msalimcengiz.saga.messages.Topics;
import com.msalimcengiz.saga.testsupport.Infra;
import com.msalimcengiz.saga.testsupport.KafkaProbe;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootTest(properties = "saga.outbox.poll-interval-ms=50")
@Import(Infra.class)
class InventoryServiceIT {

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired ConsumerFactory<String, String> consumers;
    @Autowired MessageCodec codec;
    @Autowired JdbcClient jdbc;

    @Test
    void neverOversellsUnderConcurrentReservations() {
        // 40 orders for 3 units each compete for 100 units, spread over all partitions
        // and consumer threads: exactly 33 fit, and the stock must end at 1, never below 0.
        try (var probe = new KafkaProbe(kafka, consumers, codec, Topics.INVENTORY_EVENTS)) {
            Set<UUID> orders = new HashSet<>();
            IntStream.range(0, 40).parallel().forEach(i -> {
                var id = UUID.randomUUID();
                synchronized (orders) { orders.add(id); }
                probe.send(new ReserveStock(id, "p005", 3));
            });

            var replies = probe.await(Topics.INVENTORY_EVENTS, r -> orders.contains(UUID.fromString(r.key())), 40, Duration.ofSeconds(30))
                .stream().map(probe::decode).toList();

            assertThat(replies.stream().filter(StockReserved.class::isInstance)).hasSize(33);
            assertThat(replies.stream().filter(StockRejected.class::isInstance)).hasSize(7);
            assertThat(jdbc.sql("SELECT available FROM products WHERE id = 'p005'").query(Integer.class).single()).isEqualTo(1);
        }
    }

    @Test
    void repeatedCommandRepeatsTheOriginalAnswer() {
        try (var probe = new KafkaProbe(kafka, consumers, codec, Topics.INVENTORY_EVENTS)) {
            var order = UUID.randomUUID();
            probe.send(new ReserveStock(order, "p006", 60));
            probe.send(new ReserveStock(order, "p006", 60));

            var replies = probe.await(Topics.INVENTORY_EVENTS, KafkaProbe.forOrder(order), 2, Duration.ofSeconds(20));
            assertThat(replies).extracting(probe::decode).containsOnly((Message) new StockReserved(order));
            assertThat(jdbc.sql("SELECT available FROM products WHERE id = 'p006'").query(Integer.class).single()).isEqualTo(40);
        }
    }
}
