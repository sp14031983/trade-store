package com.db.controller;

import com.db.dto.TradeDto;
import com.db.exception.InvalidTradeException;
import com.db.model.Trade;
import com.db.service.TradeService;
import com.db.stream.TradeProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(TradeController.class)
class TradeControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private TradeService tradeService;

    @MockitoBean
    private TradeProducer tradeProducer;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void shouldReturn201WhenTradeCreatedSuccessfully() throws Exception {
        // Given
        UUID tradeId = UUID.randomUUID();
        TradeDto tradeDto = TradeDto.builder()
                .tradeId(tradeId)
                .version(1)
                .bookId("B1")
                .counterPartyId("CP1")
                .maturityDate(LocalDate.now().plusDays(30))
                .build();

        Trade savedTrade = Trade.builder()
                .tradeId(tradeId)
                .version(1)
                .bookId("B1")
                .counterPartyId("CP1")
                .maturityDate(LocalDate.now().plusDays(30))
                .createdDate(LocalDate.now())
                .expired(false)
                .build();

        given(tradeService.saveTrade(any(TradeDto.class))).willReturn(savedTrade);

        // When & Then
        mockMvc.perform(post("/api/trades")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(tradeDto)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.tradeId").value(tradeId.toString()))
                .andExpect(jsonPath("$.version").value(1))
                .andExpect(jsonPath("$.bookId").value("B1"))
                .andExpect(jsonPath("$.counterPartyId").value("CP1"))
                .andExpect(jsonPath("$.expired").value(false));
    }

    @Test
    void shouldReturn400WhenValidationFails() throws Exception {
        // Given
        TradeDto invalidDto = TradeDto.builder()
                .tradeId(UUID.randomUUID())
                .version(1)
                .bookId("B1")
                .counterPartyId("CP1")
                .maturityDate(LocalDate.now().minusDays(1)) // Past date
                .build();

        given(tradeService.saveTrade(any(TradeDto.class)))
                .willThrow(new InvalidTradeException("Trade maturity date cannot be in the past"));

        // When & Then
        mockMvc.perform(post("/api/trades")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(invalidDto)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Invalid Trade"))
                .andExpect(jsonPath("$.message").value("Trade maturity date cannot be in the past"));
    }

    @Test
    void shouldReturn400WhenLowerVersionProvided() throws Exception {
        // Given
        TradeDto lowerVersionDto = TradeDto.builder()
                .tradeId(UUID.randomUUID())
                .version(1)
                .bookId("B1")
                .counterPartyId("CP1")
                .maturityDate(LocalDate.now().plusDays(30))
                .build();

        given(tradeService.saveTrade(any(TradeDto.class)))
                .willThrow(new InvalidTradeException("Trade version is lower than existing version"));

        // When & Then
        mockMvc.perform(post("/api/trades")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(lowerVersionDto)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Invalid Trade"))
                .andExpect(jsonPath("$.message").value("Trade version is lower than existing version"));
    }

    @Test
    void shouldReturn500WhenUnexpectedErrorOccurs() throws Exception {
        // Given
        TradeDto tradeDto = TradeDto.builder()
                .tradeId(UUID.randomUUID())
                .version(1)
                .bookId("B1")
                .counterPartyId("CP1")
                .maturityDate(LocalDate.now().plusDays(30))
                .build();

        given(tradeService.saveTrade(any(TradeDto.class)))
                .willThrow(new RuntimeException("Database connection failed"));

        // When & Then
        mockMvc.perform(post("/api/trades")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(tradeDto)))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("Internal Error"));
    }

    @Test
    void shouldReturnAllTradesSuccessfully() throws Exception {
        // Given
        List<Trade> trades = Arrays.asList(
                Trade.builder()
                        .tradeId(UUID.randomUUID())
                        .version(1)
                        .bookId("B1")
                        .counterPartyId("CP1")
                        .maturityDate(LocalDate.now().plusDays(30))
                        .createdDate(LocalDate.now())
                        .expired(false)
                        .build(),
                Trade.builder()
                        .tradeId(UUID.randomUUID())
                        .version(2)
                        .bookId("B2")
                        .counterPartyId("CP2")
                        .maturityDate(LocalDate.now().plusDays(60))
                        .createdDate(LocalDate.now().minusDays(5))
                        .expired(false)
                        .build()
        );

        given(tradeService.getAllTrades()).willReturn(trades);

        // When & Then
        mockMvc.perform(get("/api/trades"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].bookId").value("B1"))
                .andExpect(jsonPath("$[1].bookId").value("B2"))
                .andExpect(jsonPath("$[0].version").value(1))
                .andExpect(jsonPath("$[1].version").value(2));
    }

    @Test
    void shouldReturnEmptyListWhenNoTrades() throws Exception {
        // Given
        given(tradeService.getAllTrades()).willReturn(Arrays.asList());

        // When & Then
        mockMvc.perform(get("/api/trades"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    void shouldPublishTradeToKafkaSuccessfully() throws Exception {
        // Given
        TradeDto tradeDto = TradeDto.builder()
                .tradeId(UUID.randomUUID())
                .version(1)
                .bookId("B1")
                .counterPartyId("CP1")
                .maturityDate(LocalDate.now().plusDays(30))
                .build();

        doNothing().when(tradeProducer).publishTrade(any(TradeDto.class));

        // When & Then
        mockMvc.perform(post("/api/trades/publish")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(tradeDto)))
                .andExpect(status().isOk())
                .andExpect(content().string("Trade published to Kafka successfully"));
    }

    @Test
    void shouldHandleKafkaPublishingError() throws Exception {
        // Given
        TradeDto tradeDto = TradeDto.builder()
                .tradeId(UUID.randomUUID())
                .version(1)
                .bookId("B1")
                .counterPartyId("CP1")
                .maturityDate(LocalDate.now().plusDays(30))
                .build();

        doThrow(new RuntimeException("Kafka is down"))
                .when(tradeProducer).publishTrade(any(TradeDto.class));

        // When & Then
        mockMvc.perform(post("/api/trades/publish")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(tradeDto)))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("Internal Error"));
    }

    @Test
    void shouldHandleInvalidJson() throws Exception {
        // When & Then
        mockMvc.perform(post("/api/trades")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("invalid-json"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldHandleMissingRequestBody() throws Exception {
        // When & Then
        mockMvc.perform(post("/api/trades")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());
    }
}