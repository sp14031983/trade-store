package com.db.repository;

import com.db.model.Trade;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface TradeRepository extends JpaRepository<Trade, UUID> {
    Optional<Trade> findByBookIdAndCounterPartyId(String bookId, String counterPartyId);
    List<Trade> findByExpiredFalseAndMaturityDateBefore(LocalDate date);
}
