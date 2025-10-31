package com.db.service;

import com.db.dto.TradeDto;
import com.db.exception.InvalidTradeException;
import com.db.model.Trade;
import com.db.model.TradeHistory;
import com.db.repository.TradeHistoryRepository;
import com.db.repository.TradeRepository;
import com.db.stream.TradeProducer;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class TradeService {

    private final TradeRepository tradeRepository;
    private final TradeHistoryRepository tradeHistoryRepository;
    private final TradeProducer tradeProducer;
    private static final Logger log = LoggerFactory.getLogger(TradeService.class);

    @Transactional
    public Trade saveTrade(TradeDto dto) {
        if (dto.getMaturityDate().isBefore(LocalDate.now())) {
            throw new InvalidTradeException("Trade maturity date cannot be in the past");
        }

        Optional<Trade> existing = tradeRepository.findById(dto.getTradeId());
        Trade tradeToSave;

        if (existing.isPresent()) {
            Trade oldTrade = existing.get();

            if (dto.getVersion() < oldTrade.getVersion()) {
                throw new InvalidTradeException("Trade version is lower than existing version");
            }

            // If same version, update existing
            tradeToSave = oldTrade;
            tradeToSave.setVersion(dto.getVersion());
            tradeToSave.setBookId(dto.getBookId());
            tradeToSave.setCounterPartyId(dto.getCounterPartyId());
            tradeToSave.setMaturityDate(dto.getMaturityDate());
            tradeToSave.setExpired(false);
        } else {
            // new trade
            tradeToSave = new Trade();
            tradeToSave.setTradeId(dto.getTradeId() == null ? UUID.randomUUID() : dto.getTradeId());
            tradeToSave.setVersion(dto.getVersion());
            tradeToSave.setBookId(dto.getBookId());
            tradeToSave.setCounterPartyId(dto.getCounterPartyId());
            tradeToSave.setMaturityDate(dto.getMaturityDate());
            tradeToSave.setCreatedDate(LocalDate.now());
            tradeToSave.setExpired(false);
        }

        Trade savedTrade = tradeRepository.save(tradeToSave);
        saveTradeHistory(savedTrade);

        return savedTrade;
    }

    // Circuit breaker for MongoDB operations
    @CircuitBreaker(name = "mongodb", fallbackMethod = "saveHistoryFallback")
    public void saveTradeHistory(Trade trade) {
        // Save history to MongoDB
        TradeHistory history = TradeHistory.builder()
                .id(UUID.randomUUID())
                .tradeId(trade.getTradeId())
                .version(trade.getVersion())
                .bookId(trade.getBookId())
                .counterPartyId(trade.getCounterPartyId())
                .maturityDate(trade.getMaturityDate())
                .createdDate(trade.getCreatedDate())
                .expired(trade.isExpired())
                .recordedDate(LocalDate.now())
                .build();

        tradeHistoryRepository.save(history);
    }

    public void saveHistoryFallback(Trade trade, Exception ex) {
        log.error("Failed to save trade history for {}", trade.getTradeId(), ex);
        // Could save to retry queue or alternative storage
    }

    public List<Trade> getAllTrades() {
        return tradeRepository.findAll();
    }

    @Transactional
    public void markExpiredTrades() {
        List<Trade> expiredTrades = tradeRepository.findByExpiredFalseAndMaturityDateBefore(LocalDate.now());
        if(expiredTrades == null || expiredTrades.isEmpty()) return;
        expiredTrades.forEach(trade -> trade.setExpired(true));
        tradeRepository.saveAll(expiredTrades);
    }

    // Circuit breaker for Kafka publishing
    @CircuitBreaker(name = "kafka", fallbackMethod = "publishTradeFallback")
    @Retry(name = "kafka")
    public void publishTradeEvent(TradeDto dto) {
        tradeProducer.publishTrade(dto);
    }

    // Fallback method
    public void publishTradeFallback(TradeDto dto, Exception ex) {

        log.error("Failed to publish trade {} to Kafka, storing for retry", dto.getTradeId(), ex);
        // Store in retry queue or dead letter table
    }

}
