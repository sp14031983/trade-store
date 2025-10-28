package com.db.stream;

import com.db.dto.TradeDto;
import com.db.service.TradeService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TradeConsumer {

    private final TradeService tradeService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${app.kafka.topic:trades}", groupId = "trade-group")
    public void consumeTrade(String message) {
        try {
            TradeDto dto = objectMapper.readValue(message, TradeDto.class);
            tradeService.saveTrade(dto);
            log.info("Consumed trade: {}", dto.getTradeId());
        } catch (Exception e) {
            log.error("Failed to process trade message: {}", message, e);
        }
    }
}
