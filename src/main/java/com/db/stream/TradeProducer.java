package com.db.stream;

import com.db.dto.TradeDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TradeProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic:trades}")
    private String tradeTopic;

    public void publishTrade(TradeDto dto) {
        try {
            String json = objectMapper.writeValueAsString(dto);
            kafkaTemplate.send(tradeTopic, json);
            log.info("📤 Published trade: {}", dto.getTradeId());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize trade for Kafka", e);
        }
    }
}
