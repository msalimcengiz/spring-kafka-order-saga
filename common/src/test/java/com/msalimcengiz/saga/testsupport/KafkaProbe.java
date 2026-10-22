package com.msalimcengiz.saga.testsupport;

import com.msalimcengiz.saga.messages.Envelope;
import com.msalimcengiz.saga.messages.Message;
import com.msalimcengiz.saga.messages.MessageCodec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/** Sends messages the way another service would and reads what the service under test publishes. */
public class KafkaProbe implements AutoCloseable {

    private final KafkaTemplate<String, String> kafka;
    private final MessageCodec codec;
    private final Consumer<String, String> consumer;
    private final List<ConsumerRecord<String, String>> seen = new ArrayList<>();

    public KafkaProbe(KafkaTemplate<String, String> kafka, ConsumerFactory<String, String> factory, MessageCodec codec, String... topics) {
        this.kafka = kafka;
        this.codec = codec;
        var props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        this.consumer = factory.createConsumer("probe-" + UUID.randomUUID(), null, null, props);
        this.consumer.subscribe(List.of(topics));
        // Join the group and get assignments before the test produces anything.
        long deadline = System.currentTimeMillis() + 15_000;
        while (consumer.assignment().isEmpty() && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(100));
        }
    }

    public Envelope send(Message m) {
        return send(Envelope.of(m));
    }

    public Envelope send(Envelope e) {
        sendRaw(e.message().topic(), e.message().orderId().toString(), codec.encode(e));
        return e;
    }

    public void sendRaw(String topic, String key, String value) {
        try {
            kafka.send(topic, key, value).get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    /** Polls until {@code n} records on {@code topic} match, or fails after the timeout. */
    public List<ConsumerRecord<String, String>> await(String topic, Predicate<ConsumerRecord<String, String>> match, int n, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(200)).forEach(seen::add);
            var hits = seen.stream().filter(r -> r.topic().equals(topic) && match.test(r)).toList();
            if (hits.size() >= n) {
                return hits;
            }
        }
        throw new AssertionError("expected " + n + " matching records on " + topic + ", saw " +
            seen.stream().filter(r -> r.topic().equals(topic) && match.test(r)).count());
    }

    /** Collects for {@code window} and returns every matching record (to assert there are no extras). */
    public List<ConsumerRecord<String, String>> collect(String topic, Predicate<ConsumerRecord<String, String>> match, Duration window) {
        long end = System.currentTimeMillis() + window.toMillis();
        while (System.currentTimeMillis() < end) {
            consumer.poll(Duration.ofMillis(200)).forEach(seen::add);
        }
        return seen.stream().filter(r -> r.topic().equals(topic) && match.test(r)).toList();
    }

    public Message decode(ConsumerRecord<String, String> r) {
        return codec.decode(r.value()).message();
    }

    public static Predicate<ConsumerRecord<String, String>> forOrder(UUID orderId) {
        return r -> orderId.toString().equals(r.key());
    }

    @Override
    public void close() {
        consumer.close();
    }
}
