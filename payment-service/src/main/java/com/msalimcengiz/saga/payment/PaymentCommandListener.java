package com.msalimcengiz.saga.payment;

import com.msalimcengiz.saga.inbox.Inbox;
import com.msalimcengiz.saga.messages.MessageCodec;
import com.msalimcengiz.saga.messages.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class PaymentCommandListener {

    private final MessageCodec codec;
    private final Inbox inbox;
    private final PaymentHandler handler;

    public PaymentCommandListener(MessageCodec codec, Inbox inbox, PaymentHandler handler) {
        this.codec = codec;
        this.inbox = inbox;
        this.handler = handler;
    }

    @KafkaListener(topics = Topics.PAYMENT_COMMANDS, groupId = "payment-service")
    @Transactional
    public void onCommand(ConsumerRecord<String, String> record) {
        var envelope = codec.decode(record.value());
        if (inbox.firstDelivery(envelope.messageId(), record.topic())) {
            handler.handle(envelope.message());
        }
    }
}
