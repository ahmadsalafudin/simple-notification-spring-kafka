package com.filkod.spring.notification.kafka.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.filkod.spring.notification.kafka.dto.OrderRequest;
import com.filkod.spring.notification.kafka.dto.Response;
import com.filkod.spring.notification.kafka.message.OrderMessageProducer;
import com.filkod.spring.notification.kafka.model.Order;
import com.filkod.spring.notification.kafka.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderMessageProducer messageProducer;
    private final OrderRepository orderRepository;
    private final ObjectMapper objectMapper;
    private final static String SUCCESS = "Success";
    private final static String ERROR = "Error";
    private final static String OPEN = "OPEN";
    private final static String FAILED = "FAILED";

    public Response doInsertOrder(OrderRequest request) {
        Response response = new Response();
        Order order = new Order();
        response.setStatus(SUCCESS);
        response.setMessage("Order created");

        Optional<Order> byTransactionId = orderRepository.findOrderByTransactionId(request.getTransactionId());
        if (byTransactionId.isPresent()) {
            log.info("transactionID already exist >> " + byTransactionId.get().getTransactionId());
            response.setStatus(ERROR);
            response.setMessage("Transaction ID already exist");
            return response;
        }

        try {
            order.setTransactionId(request.getTransactionId());
            order.setProductName(request.getProductName());
            order.setProductQty(request.getProductQty());
            order.setProductPrice(request.getProductPrice());
            order.setBuyerName(request.getBuyerName());
            order.setBuyerPhoneNumber(request.getBuyerPhoneNumber());
            order.setBuyerEmail(request.getBuyerEmail());
            order.setState(OPEN);
            orderRepository.save(order);
            log.info("successfully save data to order >> " + order.getId());
        } catch (Exception er) {
            log.info("failed save data to order >> " + er.getMessage());
            response.setStatus(ERROR);
            response.setMessage("Order failed");
        }

        try {
            String payload = objectMapper.writeValueAsString(order);
            this.messageProducer.sendMessage(payload);
            log.info("Successfully publish : {}", payload);
        } catch (Exception er) {
            log.info("failed save data to order >> " + er.getMessage());
            Optional<Order> orderByTransactionId = orderRepository.findOrderByTransactionId(request.getTransactionId());
            orderByTransactionId.get().setState(FAILED);
            orderRepository.save(orderByTransactionId.get());
            response.setStatus(ERROR);
            response.setMessage("Order failed");
        }

        return response;
    }

    public Response doCheckStatus(String transactionId) {
        Response response = new Response();
        Optional<Order> byTransactionId = orderRepository.findOrderByTransactionId(transactionId);
        if (byTransactionId.isPresent()) {
            log.info("transactionID already exist >> " + byTransactionId.get().getTransactionId());
            response.setStatus(SUCCESS);
            response.setMessage("State order with transaction ID " + transactionId + " is " + byTransactionId.get().getState());
        } else {
            response.setStatus(ERROR);
            response.setMessage("Transaction ID not found");
        }

        return response;
    }
}
