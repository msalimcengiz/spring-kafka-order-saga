package com.msalimcengiz.saga.inventory;

import com.msalimcengiz.saga.inbox.Inbox;
import com.msalimcengiz.saga.messages.MessageCodec;
import com.msalimcengiz.saga.messages.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class InventoryCommandListener {

    private final MessageCodec codec;
    private final Inbox inbox;
    private final InventoryHandler handler;

    public InventoryCommandListener(MessageCodec codec, Inbox inbox, InventoryHandler handler) {
        this.codec = codec;
        this.inbox = inbox;
        this.handler = handler;
    }

    @KafkaListener(topics = Topics.INVENTORY_COMMANDS, groupId = "inventory-service")
    @Transactional
    public void onCommand(ConsumerRecord<String, String> record) {
        var envelope = codec.decode(record.value());
        if (inbox.firstDelivery(envelope.messageId(), record.topic())) {
            handler.handle(envelope.message());
        }
    }
}
