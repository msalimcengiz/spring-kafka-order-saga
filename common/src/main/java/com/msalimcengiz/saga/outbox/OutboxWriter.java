package com.msalimcengiz.saga.outbox;

import com.msalimcengiz.saga.messages.Envelope;
import com.msalimcengiz.saga.messages.Message;
import com.msalimcengiz.saga.messages.MessageCodec;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Records a message to publish. Must run inside the caller's transaction: the message is
 * stored if and only if the business change commits. This replaces "write the database,
 * then publish to Kafka", which loses the message when the process dies in between.
 */
@Component
public class OutboxWriter {

    private final JdbcClient jdbc;
    private final MessageCodec codec;

    public OutboxWriter(JdbcClient jdbc, MessageCodec codec) {
        this.jdbc = jdbc;
        this.codec = codec;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void append(Message message) {
        Envelope envelope = Envelope.of(message);
        jdbc.sql("INSERT INTO outbox (message_id, topic, msg_key, payload) VALUES (?, ?, ?, ?)")
            .params(envelope.messageId(), message.topic(), message.orderId().toString(), codec.encode(envelope))
            .update();
    }
}
