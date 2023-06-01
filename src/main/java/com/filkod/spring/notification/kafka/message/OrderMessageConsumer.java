package com.filkod.spring.notification.kafka.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.filkod.spring.notification.kafka.dto.OrderRequest;
import com.filkod.spring.notification.kafka.model.Order;
import com.filkod.spring.notification.kafka.repository.OrderRepository;
import com.filkod.spring.notification.kafka.service.MailSenderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderMessageConsumer  {

    @Value("${partner.url.order}")
    private String urlOrder;
    private final ObjectMapper objectMapper;
    private final RestTemplate restTemplate;
    private final OrderRepository orderRepository;
    private final MailSenderService mailSenderService;
    private final static String SENT = "SENT";

    @KafkaListener(topics = "${spring.kafka.topic.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void orderConsume(String message){
        try {
            log.info("Incoming request order ....");
            OrderRequest msg = objectMapper.readValue(message, OrderRequest.class);
            log.info("Message: {}", msg.toString());
            String transactionId = msg.getTransactionId();
            String buyerMail = msg.getBuyerEmail();

            HttpHeaders httpHeaders = new HttpHeaders();
            httpHeaders.setContentType(MediaType.APPLICATION_JSON);
            httpHeaders.add("X-API-Key", "apiKey");
            HttpEntity<?> httpEntity = new HttpEntity<>(msg, httpHeaders);
            log.info("HttpEntity to partner: {}", httpEntity.getBody());
            ResponseEntity<String> response = restTemplate.exchange(
                    urlOrder,
                    HttpMethod.POST,
                    httpEntity,
                    String.class
            );

            log.info("Response Status From Partner: {}", response.getStatusCode());

            if (response.getStatusCode().equals(HttpStatus.OK)) {
                Optional<Order> orderByTransactionId = orderRepository.findOrderByTransactionId(transactionId);
                orderByTransactionId.get().setState(SENT);
                orderRepository.save(orderByTransactionId.get());
                mailSenderService.sendMailNotification(buyerMail, "Your order request is successfully, please check status for update", "Order Is Successfully");
                log.info("Order completed: {}", orderByTransactionId.get().getTransactionId());

                // callback to client
            } else {
                log.info("Response error from partner : {}", response.getStatusCode());
                // callback to client
            }
        } catch (IOException e) {
            log.error("Couldn't convert json", e);
        }
    }

}
