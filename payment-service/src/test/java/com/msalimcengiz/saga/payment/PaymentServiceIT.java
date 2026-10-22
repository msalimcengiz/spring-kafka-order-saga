package com.msalimcengiz.saga.payment;

import static com.msalimcengiz.saga.testsupport.KafkaProbe.forOrder;
import static org.assertj.core.api.Assertions.assertThat;

import com.msalimcengiz.saga.kafka.DltReplayController;
import com.msalimcengiz.saga.messages.Envelope;
import com.msalimcengiz.saga.messages.Message.ChargePayment;
import com.msalimcengiz.saga.messages.Message.PaymentCharged;
import com.msalimcengiz.saga.messages.Message.PaymentDeclined;
import com.msalimcengiz.saga.messages.Message.PaymentRefunded;
import com.msalimcengiz.saga.messages.Message.RefundPayment;
import com.msalimcengiz.saga.messages.MessageCodec;
import com.msalimcengiz.saga.messages.Topics;
import com.msalimcengiz.saga.testsupport.Infra;
import com.msalimcengiz.saga.testsupport.KafkaProbe;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;

@SpringBootTest(properties = "saga.outbox.poll-interval-ms=50")
@Import(Infra.class)
class PaymentServiceIT {

    private static final Duration WAIT = Duration.ofSeconds(20);

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired ConsumerFactory<String, String> consumers;
    @Autowired MessageCodec codec;
    @Autowired JdbcClient jdbc;
    @Autowired DltReplayController dlt;

    KafkaProbe probe;

    @BeforeEach
    void probe() {
        probe = new KafkaProbe(kafka, consumers, codec, Topics.PAYMENT_EVENTS, Topics.deadLetter(Topics.PAYMENT_COMMANDS));
    }

    @AfterEach
    void close() {
        probe.close();
    }

    BigDecimal balance(String customer) {
        return jdbc.sql("SELECT balance FROM accounts WHERE customer_id = ?").param(customer).query(BigDecimal.class).single();
    }

    @Test
    void chargesOncePerOrderDespiteRedeliveryAndResend() {
        var order = UUID.randomUUID();
        var cmd = Envelope.of(new ChargePayment(order, "c010", new BigDecimal("250.00")));

        probe.send(cmd);
        probe.send(cmd);                                   // redelivery: same message id
        probe.send(new ChargePayment(order, "c010", new BigDecimal("250.00"))); // saga re-sent it: new id

        var replies = probe.await(Topics.PAYMENT_EVENTS, forOrder(order), 2, WAIT);
        assertThat(replies).extracting(probe::decode).containsOnly(new PaymentCharged(order, new BigDecimal("250.00")));
        assertThat(probe.collect(Topics.PAYMENT_EVENTS, forOrder(order), Duration.ofSeconds(2))).hasSize(2);
        assertThat(balance("c010")).isEqualByComparingTo("4750.00");
    }

    @Test
    void declinesWhenFundsAreInsufficientAndKeepsTheBalance() {
        var order = UUID.randomUUID();
        probe.send(new ChargePayment(order, "c011", new BigDecimal("5000.01")));

        var reply = probe.decode(probe.await(Topics.PAYMENT_EVENTS, forOrder(order), 1, WAIT).getFirst());
        assertThat(reply).isEqualTo(new PaymentDeclined(order, "insufficient_funds"));
        assertThat(balance("c011")).isEqualByComparingTo("5000.00");
    }

    @Test
    void refundsOnceAndRestoresTheBalance() {
        var order = UUID.randomUUID();
        probe.send(new ChargePayment(order, "c012", new BigDecimal("400.00")));
        probe.await(Topics.PAYMENT_EVENTS, forOrder(order), 1, WAIT);

        probe.send(new RefundPayment(order));
        probe.send(new RefundPayment(order));

        var replies = probe.await(Topics.PAYMENT_EVENTS, r -> forOrder(order).test(r) && probe.decode(r) instanceof PaymentRefunded, 2, WAIT);
        assertThat(replies).extracting(probe::decode).containsOnly(new PaymentRefunded(order, new BigDecimal("400.00")));
        assertThat(balance("c012")).isEqualByComparingTo("5000.00");
    }

    @Test
    void malformedRecordGoesToTheDltWithoutBlockingThePartition() {
        var key = UUID.randomUUID();
        probe.sendRaw(Topics.PAYMENT_COMMANDS, key.toString(), "{not json");
        // Same key, so same partition: it must not be stuck behind the poison record.
        probe.send(new ChargePayment(key, "c013", new BigDecimal("1.00")));

        var dead = probe.await(Topics.deadLetter(Topics.PAYMENT_COMMANDS), forOrder(key), 1, WAIT).getFirst();
        assertThat(dead.value()).isEqualTo("{not json");
        assertThat(new String(dead.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE).value(), StandardCharsets.UTF_8))
            .contains("cannot decode message");
        probe.await(Topics.PAYMENT_EVENTS, forOrder(key), 1, WAIT);
    }

    @Test
    void dltReplayRepublishesToTheOriginalTopic() throws Exception {
        // A command that was dead-lettered earlier (say, during an outage) and is now fixable.
        var order = UUID.randomUUID();
        var record = new ProducerRecord<String, String>(Topics.deadLetter(Topics.PAYMENT_COMMANDS), order.toString(),
            codec.encode(Envelope.of(new ChargePayment(order, "c014", new BigDecimal("10.00")))));
        record.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC, Topics.PAYMENT_COMMANDS.getBytes(StandardCharsets.UTF_8));
        kafka.send(record).get();

        // A poison record in the same DLT: replaying it fails again and returns it to the
        // DLT; the replay must still finish instead of looping on it.
        var poison = new ProducerRecord<String, String>(Topics.deadLetter(Topics.PAYMENT_COMMANDS), UUID.randomUUID().toString(), "{still bad");
        poison.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC, Topics.PAYMENT_COMMANDS.getBytes(StandardCharsets.UTF_8));
        kafka.send(poison).get();

        assertThat(dlt.replay("wrong", Topics.PAYMENT_COMMANDS, 100).getStatusCode().value()).isEqualTo(401);
        long start = System.currentTimeMillis();
        var res = dlt.replay("dev-admin-token", Topics.PAYMENT_COMMANDS, 1000);
        assertThat(System.currentTimeMillis() - start).isLessThan(15_000);
        assertThat((int) res.getBody().get("replayed")).isGreaterThanOrEqualTo(2);

        probe.await(Topics.PAYMENT_EVENTS, forOrder(order), 1, WAIT);
        assertThat(balance("c014")).isEqualByComparingTo("4990.00");
    }
}
