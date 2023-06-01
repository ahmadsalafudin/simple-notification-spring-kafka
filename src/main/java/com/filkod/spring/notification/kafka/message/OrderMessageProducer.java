package com.filkod.spring.notification.kafka.message;

import com.filkod.spring.notification.kafka.dto.OrderRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderMessageProducer {
    @Value("${spring.kafka.topic.name}")
    private String topicJsonName;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String data){
        log.info("Message sent {}", data);
        Message<String> message = MessageBuilder
                .withPayload(data)
                .setHeader(KafkaHeaders.TOPIC, topicJsonName)
                .build();
        kafkaTemplate.send(message);
    }
}
