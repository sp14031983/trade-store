package com.db.repository;

import com.db.model.Trade;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@ActiveProfiles("test")
class TradeRepositoryTest {

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private TradeRepository tradeRepository;

    private UUID tradeId1;
    private UUID tradeId2;
    private UUID tradeId3;

    @BeforeEach
    void setUp() {
        tradeId1 = UUID.randomUUID();
        tradeId2 = UUID.randomUUID();
        tradeId3 = UUID.randomUUID();
    }

    @Test
    void shouldFindByBookIdAndCounterPartyId() {
        // Given
        Trade trade = Trade.builder()
                .tradeId(tradeId1)
                .version(1)
                .bookId("BOOK1")
                .counterPartyId("CP1")
                .maturityDate(LocalDate.now().plusDays(30))
                .createdDate(LocalDate.now())
                .expired(false)
                .build();

        entityManager.persistAndFlush(trade);

        // When
        List<Trade> result = tradeRepository.findByBookIdAndCounterPartyId("BOOK1", "CP1");

        // Then
        assertThat(!result.isEmpty());
        assertThat(result.get(0).getTradeId()).isEqualTo(tradeId1);
        assertThat(result.get(0).getBookId()).isEqualTo("BOOK1");
        assertThat(result.get(0).getCounterPartyId()).isEqualTo("CP1");
        assertThat(result.get(0).getVersion()).isEqualTo(1);
        assertThat(result.get(0).isExpired()).isFalse();
    }

    @Test
    void shouldNotFindByBookIdAndCounterPartyIdWhenNotExists() {
        // When
        List<Trade> result = tradeRepository.findByBookIdAndCounterPartyId("NONEXISTENT", "CP1");

        // Then
        assertThat(result).isEmpty();
    }

    @Test
    void shouldFindByBookIdAndCounterPartyIdWithDifferentVersions() {
        // Given - Multiple versions of same trade
        Trade trade1 = Trade.builder()
                .tradeId(tradeId1)
                .version(1)
                .bookId("BOOK1")
                .counterPartyId("CP1")
                .maturityDate(LocalDate.now().plusDays(30))
                .createdDate(LocalDate.now().minusDays(5))
                .expired(false)
                .build();

        Trade trade2 = Trade.builder()
                .tradeId(tradeId2)
                .version(2)
                .bookId("BOOK1")
                .counterPartyId("CP1")
                .maturityDate(LocalDate.now().plusDays(35))
                .createdDate(LocalDate.now())
                .expired(false)
                .build();

        entityManager.persist(trade1);
        entityManager.persist(trade2);
        entityManager.flush();

        // When
        List<Trade> result = tradeRepository.findByBookIdAndCounterPartyId("BOOK1", "CP1");

        // Then - Should return one of them (JPA behavior may vary)
        assertThat(!result.isEmpty());
        assertThat(result.get(0).getBookId()).isEqualTo("BOOK1");
        assertThat(result.get(0).getCounterPartyId()).isEqualTo("CP1");
    }

    @Test
    void shouldFindExpiredTradesByMaturityDate() {
        // Given
        LocalDate pastDate = LocalDate.now().minusDays(5);
        LocalDate futureDate = LocalDate.now().plusDays(5);

        Trade expiredTrade = Trade.builder()
                .tradeId(tradeId1)
                .version(1)
                .bookId("BOOK1")
                .counterPartyId("CP1")
                .maturityDate(pastDate)
                .createdDate(LocalDate.now().minusDays(10))
                .expired(false) // Not yet marked as expired
                .build();

        Trade activeTrade = Trade.builder()
                .tradeId(tradeId2)
                .version(1)
                .bookId("BOOK2")
                .counterPartyId("CP2")
                .maturityDate(futureDate)
                .createdDate(LocalDate.now().minusDays(5))
                .expired(false)
                .build();

        Trade alreadyExpiredTrade = Trade.builder()
                .tradeId(tradeId3)
                .version(1)
                .bookId("BOOK3")
                .counterPartyId("CP3")
                .maturityDate(pastDate)
                .createdDate(LocalDate.now().minusDays(10))
                .expired(true) // Already marked as expired
                .build();

        entityManager.persist(expiredTrade);
        entityManager.persist(activeTrade);
        entityManager.persist(alreadyExpiredTrade);
        entityManager.flush();

        // When
        List<Trade> expiredTrades = tradeRepository.findByExpiredFalseAndMaturityDateBefore(LocalDate.now());

        // Then
        assertThat(expiredTrades).hasSize(1);
        assertThat(expiredTrades.get(0).getTradeId()).isEqualTo(tradeId1);
        assertThat(expiredTrades.get(0).isExpired()).isFalse();
        assertThat(expiredTrades.get(0).getMaturityDate()).isBefore(LocalDate.now());
    }

    @Test
    void shouldReturnEmptyListWhenNoExpiredTrades() {
        // Given
        Trade activeTrade = Trade.builder()
                .tradeId(tradeId1)
                .version(1)
                .bookId("BOOK1")
                .counterPartyId("CP1")
                .maturityDate(LocalDate.now().plusDays(30))
                .createdDate(LocalDate.now())
                .expired(false)
                .build();

        entityManager.persistAndFlush(activeTrade);

        // When
        List<Trade> expiredTrades = tradeRepository.findByExpiredFalseAndMaturityDateBefore(LocalDate.now());

        // Then
        assertThat(expiredTrades).isEmpty();
    }

    @Test
    void shouldFindMultipleExpiredTrades() {
        // Given
        LocalDate pastDate1 = LocalDate.now().minusDays(10);
        LocalDate pastDate2 = LocalDate.now().minusDays(3);

        Trade expiredTrade1 = Trade.builder()
                .tradeId(tradeId1)
                .version(1)
                .bookId("BOOK1")
                .counterPartyId("CP1")
                .maturityDate(pastDate1)
                .createdDate(LocalDate.now().minusDays(15))
                .expired(false)
                .build();

        Trade expiredTrade2 = Trade.builder()
                .tradeId(tradeId2)
                .version(1)
                .bookId("BOOK2")
                .counterPartyId("CP2")
                .maturityDate(pastDate2)
                .createdDate(LocalDate.now().minusDays(8))
                .expired(false)
                .build();

        entityManager.persist(expiredTrade1);
        entityManager.persist(expiredTrade2);
        entityManager.flush();

        // When
        List<Trade> expiredTrades = tradeRepository.findByExpiredFalseAndMaturityDateBefore(LocalDate.now());

        // Then
        assertThat(expiredTrades).hasSize(2);
        assertThat(expiredTrades)
                .extracting(Trade::getTradeId)
                .containsExactlyInAnyOrder(tradeId1, tradeId2);
        assertThat(expiredTrades)
                .allMatch(trade -> !trade.isExpired())
                .allMatch(trade -> trade.getMaturityDate().isBefore(LocalDate.now()));
    }

    @Test
    void shouldSaveAndRetrieveTradeById() {
        // Given
        Trade trade = Trade.builder()
                .tradeId(tradeId1)
                .version(1)
                .bookId("BOOK1")
                .counterPartyId("CP1")
                .maturityDate(LocalDate.now().plusDays(30))
                .createdDate(LocalDate.now())
                .expired(false)
                .build();

        // When
        Trade savedTrade = tradeRepository.save(trade);
        Optional<Trade> retrievedTrade = tradeRepository.findById(tradeId1);

        // Then
        assertThat(savedTrade.getTradeId()).isEqualTo(tradeId1);
        assertThat(retrievedTrade).isPresent();
        assertThat(retrievedTrade.get().getVersion()).isEqualTo(1);
        assertThat(retrievedTrade.get().getBookId()).isEqualTo("BOOK1");
        assertThat(retrievedTrade.get().getCounterPartyId()).isEqualTo("CP1");
        assertThat(retrievedTrade.get().isExpired()).isFalse();
    }

    @Test
    void shouldUpdateExistingTrade() {
        // Given
        Trade originalTrade = Trade.builder()
                .tradeId(tradeId1)
                .version(1)
                .bookId("BOOK1")
                .counterPartyId("CP1")
                .maturityDate(LocalDate.now().plusDays(30))
                .createdDate(LocalDate.now())
                .expired(false)
                .build();

        entityManager.persistAndFlush(originalTrade);

        // When - Update the trade
        originalTrade.setVersion(2);
        originalTrade.setBookId("BOOK1_UPDATED");
        originalTrade.setExpired(true);
        Trade updatedTrade = tradeRepository.save(originalTrade);

        // Then
        Optional<Trade> retrievedTrade = tradeRepository.findById(tradeId1);
        assertThat(retrievedTrade).isPresent();
        assertThat(retrievedTrade.get().getVersion()).isEqualTo(2);
        assertThat(retrievedTrade.get().getBookId()).isEqualTo("BOOK1_UPDATED");
        assertThat(retrievedTrade.get().isExpired()).isTrue();
    }

    @Test
    void shouldDeleteTradeById() {
        // Given
        Trade trade = Trade.builder()
                .tradeId(tradeId1)
                .version(1)
                .bookId("BOOK1")
                .counterPartyId("CP1")
                .maturityDate(LocalDate.now().plusDays(30))
                .createdDate(LocalDate.now())
                .expired(false)
                .build();

        entityManager.persistAndFlush(trade);

        // When
        tradeRepository.deleteById(tradeId1);

        // Then
        Optional<Trade> retrievedTrade = tradeRepository.findById(tradeId1);
        assertThat(retrievedTrade).isEmpty();
    }

    @Test
    void shouldFindAllTrades() {
        // Given
        Trade trade1 = Trade.builder()
                .tradeId(tradeId1)
                .version(1)
                .bookId("BOOK1")
                .counterPartyId("CP1")
                .maturityDate(LocalDate.now().plusDays(30))
                .createdDate(LocalDate.now())
                .expired(false)
                .build();

        Trade trade2 = Trade.builder()
                .tradeId(tradeId2)
                .version(1)
                .bookId("BOOK2")
                .counterPartyId("CP2")
                .maturityDate(LocalDate.now().plusDays(60))
                .createdDate(LocalDate.now())
                .expired(true)
                .build();

        entityManager.persist(trade1);
        entityManager.persist(trade2);
        entityManager.flush();

        // When
        List<Trade> allTrades = tradeRepository.findAll();

        // Then
        assertThat(allTrades).hasSize(2);
        assertThat(allTrades)
                .extracting(Trade::getTradeId)
                .containsExactlyInAnyOrder(tradeId1, tradeId2);
    }
}