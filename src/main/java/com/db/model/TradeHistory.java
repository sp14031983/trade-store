package com.db.model;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDate;
import java.util.UUID;

@Document(collection = "trade_history")
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TradeHistory {
    @Id
    private UUID id;
    private UUID tradeId;
    private int version;
    private String counterPartyId;
    private String bookId;
    private LocalDate maturityDate;
    private LocalDate createdDate;
    private boolean expired;
    private LocalDate recordedDate;
}
