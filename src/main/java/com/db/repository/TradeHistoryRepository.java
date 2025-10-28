package com.db.repository;

import com.db.model.TradeHistory;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface TradeHistoryRepository extends MongoRepository<TradeHistory, UUID> {
}
