package com.db.service;

import com.db.dto.TradeDto;
import com.db.exception.InvalidTradeException;
import com.db.model.Trade;
import com.db.model.TradeHistory;
import com.db.repository.TradeHistoryRepository;
import com.db.repository.TradeRepository;
import lombok.RequiredArgsConstructor;
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

        // Save history to MongoDB
        TradeHistory history = TradeHistory.builder()
                .id(UUID.randomUUID())
                .tradeId(savedTrade.getTradeId())
                .version(savedTrade.getVersion())
                .bookId(savedTrade.getBookId())
                .counterPartyId(savedTrade.getCounterPartyId())
                .maturityDate(savedTrade.getMaturityDate())
                .createdDate(savedTrade.getCreatedDate())
                .expired(savedTrade.isExpired())
                .recordedDate(LocalDate.now())
                .build();

        tradeHistoryRepository.save(history);
        return savedTrade;
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

}
