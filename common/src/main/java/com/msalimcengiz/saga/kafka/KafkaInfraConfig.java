package com.msalimcengiz.saga.kafka;

import com.msalimcengiz.saga.messages.MessageCodec.MalformedMessageException;
import com.msalimcengiz.saga.messages.Topics;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class KafkaInfraConfig {

    /**
     * Topics and their dead-letter twins, with the same partition count so a failed
     * record keeps its partition number.
     */
    @Bean
    public KafkaAdmin.NewTopics sagaTopics(
        @Value("${saga.kafka.partitions:6}") int partitions,
        @Value("${saga.kafka.replicas:1}") short replicas,
        @Value("${saga.kafka.min-insync-replicas:1}") String minInsync
    ) {
        List<NewTopic> topics = new ArrayList<>();
        for (String t : Topics.ALL) {
            for (String name : List.of(t, Topics.deadLetter(t))) {
                topics.add(TopicBuilder.name(name)
                    .partitions(partitions)
                    .replicas(replicas)
                    .config("min.insync.replicas", minInsync)
                    .build());
            }
        }
        return new KafkaAdmin.NewTopics(topics.toArray(NewTopic[]::new));
    }

    /**
     * Retries happen in place (blocking), not on separate retry topics. Retry topics would
     * let later messages for the same order overtake the failing one, and the saga relies
     * on per-order ordering. While one record backs off, its partition waits; other
     * partitions keep flowing.
     *
     * <p>Malformed records can never succeed, so they go to the DLT without retries.
     */
    @Bean
    public CommonErrorHandler sagaErrorHandler(
        KafkaTemplate<String, String> template,
        @Value("${saga.kafka.max-retries:6}") int maxRetries
    ) {
        var recoverer = new DeadLetterPublishingRecoverer(
            template, (record, ex) -> new TopicPartition(Topics.deadLetter(record.topic()), record.partition()));
        var backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
        backOff.setInitialInterval(200);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000);
        var handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(MalformedMessageException.class);
        return handler;
    }
}
