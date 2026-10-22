package com.msalimcengiz.saga.order;

import static com.msalimcengiz.saga.testsupport.KafkaProbe.forOrder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.msalimcengiz.saga.messages.Message.ChargePayment;
import com.msalimcengiz.saga.messages.Message.OrderCancelled;
import com.msalimcengiz.saga.messages.Message.OrderCompleted;
import com.msalimcengiz.saga.messages.Message.PaymentCharged;
import com.msalimcengiz.saga.messages.Message.PaymentRefunded;
import com.msalimcengiz.saga.messages.Message.RefundPayment;
import com.msalimcengiz.saga.messages.Message.ReserveStock;
import com.msalimcengiz.saga.messages.Message.StockRejected;
import com.msalimcengiz.saga.messages.Message.StockReserved;
import com.msalimcengiz.saga.messages.MessageCodec;
import com.msalimcengiz.saga.messages.Topics;
import com.msalimcengiz.saga.order.OrderSaga.Status;
import com.msalimcengiz.saga.testsupport.Infra;
import com.msalimcengiz.saga.testsupport.KafkaProbe;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootTest(properties = {"saga.outbox.poll-interval-ms=50", "saga.stuck-after=PT2S", "saga.sweep-interval-ms=500"})
@Import(Infra.class)
class OrderServiceIT {

    private static final Duration WAIT = Duration.ofSeconds(20);
    private static final BigDecimal AMOUNT = new BigDecimal("99.90");

    @Autowired OrderService service;
    @Autowired OrderRepository orders;
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired ConsumerFactory<String, String> consumers;
    @Autowired MessageCodec codec;

    KafkaProbe probe;

    @BeforeEach
    void probe() {
        probe = new KafkaProbe(kafka, consumers, codec,
            Topics.PAYMENT_COMMANDS, Topics.INVENTORY_COMMANDS, Topics.ORDER_EVENTS);
    }

    @AfterEach
    void close() {
        probe.close();
    }

    private Status status(UUID id) {
        return orders.find(id).orElseThrow().status();
    }

    @Test
    void sameIdempotencyKeyCreatesOneOrderAndOneCommand() {
        var key = "checkout-" + UUID.randomUUID();
        var first = service.place(key, "c001", "p001", 2, AMOUNT);
        var second = service.place(key, "c001", "p001", 2, AMOUNT);

        assertThat(first.created()).isTrue();
        assertThat(second.created()).isFalse();
        assertThat(second.order().id()).isEqualTo(first.order().id());

        var id = first.order().id();
        var cmd = probe.decode(probe.await(Topics.PAYMENT_COMMANDS, forOrder(id), 1, WAIT).getFirst());
        assertThat(cmd).isEqualTo(new ChargePayment(id, "c001", AMOUNT));
        assertThat(probe.collect(Topics.PAYMENT_COMMANDS, forOrder(id), Duration.ofSeconds(1))).hasSize(1);
    }

    @Test
    void completesWhenPaymentAndStockSucceed() {
        var id = service.place("k-" + UUID.randomUUID(), "c002", "p002", 1, AMOUNT).order().id();
        probe.await(Topics.PAYMENT_COMMANDS, forOrder(id), 1, WAIT);

        probe.send(new PaymentCharged(id, AMOUNT));
        assertThat(probe.decode(probe.await(Topics.INVENTORY_COMMANDS, forOrder(id), 1, WAIT).getFirst()))
            .isEqualTo(new ReserveStock(id, "p002", 1));

        probe.send(new StockReserved(id));
        assertThat(probe.decode(probe.await(Topics.ORDER_EVENTS, forOrder(id), 1, WAIT).getFirst()))
            .isEqualTo(new OrderCompleted(id));
        assertThat(status(id)).isEqualTo(Status.COMPLETED);
    }

    @Test
    void refundsAndCancelsWhenStockRunsOut() {
        var id = service.place("k-" + UUID.randomUUID(), "c003", "p003", 5, AMOUNT).order().id();
        probe.send(new PaymentCharged(id, AMOUNT));
        probe.await(Topics.INVENTORY_COMMANDS, forOrder(id), 1, WAIT);

        probe.send(new StockRejected(id, "insufficient_stock"));
        assertThat(probe.decode(probe.await(Topics.PAYMENT_COMMANDS,
            r -> forOrder(id).test(r) && probe.decode(r) instanceof RefundPayment, 1, WAIT).getFirst()))
            .isEqualTo(new RefundPayment(id));

        probe.send(new PaymentRefunded(id, AMOUNT));
        probe.send(new PaymentRefunded(id, AMOUNT)); // duplicate reply: ignored
        assertThat(probe.decode(probe.await(Topics.ORDER_EVENTS, forOrder(id), 1, WAIT).getFirst()))
            .isEqualTo(new OrderCancelled(id, "out_of_stock"));
        assertThat(probe.collect(Topics.ORDER_EVENTS, forOrder(id), Duration.ofSeconds(1))).hasSize(1);
        assertThat(orders.find(id).orElseThrow().cancelReason()).isEqualTo("out_of_stock: insufficient_stock");
    }

    @Test
    void stuckSagaResendsItsCommand() {
        var id = service.place("k-" + UUID.randomUUID(), "c004", "p004", 1, AMOUNT).order().id();
        // Nobody answers: after stuck-after (2 s) the sweeper sends ChargePayment again.
        probe.await(Topics.PAYMENT_COMMANDS, forOrder(id), 2, WAIT);
        assertThat(status(id)).isEqualTo(Status.PAYMENT_PENDING);

        probe.send(new PaymentCharged(id, AMOUNT));
        await().atMost(WAIT).until(() -> status(id) == Status.STOCK_PENDING);
    }
}
