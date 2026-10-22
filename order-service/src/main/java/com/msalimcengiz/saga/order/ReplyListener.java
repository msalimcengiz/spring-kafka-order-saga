package com.msalimcengiz.saga.order;

import com.msalimcengiz.saga.inbox.Inbox;
import com.msalimcengiz.saga.messages.MessageCodec;
import com.msalimcengiz.saga.messages.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Consumes replies from payment and inventory. The inbox insert, the saga transition and
 * the outbox rows commit together; the offset is committed afterwards.
 */
@Component
public class ReplyListener {

    private final MessageCodec codec;
    private final Inbox inbox;
    private final OrderService service;

    public ReplyListener(MessageCodec codec, Inbox inbox, OrderService service) {
        this.codec = codec;
        this.inbox = inbox;
        this.service = service;
    }

    @KafkaListener(topics = {Topics.PAYMENT_EVENTS, Topics.INVENTORY_EVENTS}, groupId = "order-service")
    @Transactional
    public void onReply(ConsumerRecord<String, String> record) {
        var envelope = codec.decode(record.value());
        if (inbox.firstDelivery(envelope.messageId(), record.topic())) {
            service.apply(envelope.message());
        }
    }
}
