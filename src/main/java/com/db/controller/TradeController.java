package com.db.controller;

import com.db.dto.TradeDto;
import com.db.model.Trade;
import com.db.service.TradeService;
import com.db.stream.TradeProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/trades")
@RequiredArgsConstructor
public class TradeController {

    private final TradeService tradeService;
    private final TradeProducer tradeProducer;

    @PostMapping
    public ResponseEntity<Trade> saveTrade(@RequestBody TradeDto dto) {
        Trade trade = tradeService.saveTrade(dto);
        return ResponseEntity.status(HttpStatus.CREATED).body(trade);
    }

    @GetMapping
    public ResponseEntity<List<Trade>> getAllTrades() {
        return ResponseEntity.ok(tradeService.getAllTrades());
    }

    @PostMapping("/publish")
    public ResponseEntity<String> publishTrade(@RequestBody TradeDto dto) {
        tradeProducer.publishTrade(dto);
        return ResponseEntity.ok("Trade published to Kafka successfully");
    }

}
