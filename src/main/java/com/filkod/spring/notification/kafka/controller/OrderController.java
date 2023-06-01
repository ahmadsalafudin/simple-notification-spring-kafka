package com.filkod.spring.notification.kafka.controller;

import com.filkod.spring.notification.kafka.service.OrderService;
import com.filkod.spring.notification.kafka.dto.OrderRequest;
import com.filkod.spring.notification.kafka.dto.Response;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RequestMapping("/order")
@RestController
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    public Response createOrder(@RequestBody OrderRequest orderRequest) {
        return orderService.doInsertOrder(orderRequest);
    }

    @GetMapping("/{id}")
    public Response checkStatus(@PathVariable("id") String transactionId) {
        return orderService.doCheckStatus(transactionId);
    }
}
