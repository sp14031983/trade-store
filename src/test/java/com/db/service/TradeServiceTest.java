package com.db.service;

import com.db.dto.TradeDto;
import com.db.exception.InvalidTradeException;
import com.db.model.Trade;
import com.db.model.TradeHistory;
import com.db.repository.TradeHistoryRepository;
import com.db.repository.TradeRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TradeServiceTest {

    @Mock
    private TradeRepository tradeRepository;
    @Mock
    private TradeHistoryRepository tradeHistoryRepository;

    @InjectMocks
    private TradeService tradeService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void shouldRejectTradeWithPastMaturityDate() {
        TradeDto dto = TradeDto.builder()
                .tradeId(UUID.randomUUID())
                .version(1)
                .bookId("B1")
                .counterPartyId("CP-1")
                .maturityDate(LocalDate.now().minusDays(1))
                .build();

        assertThrows(InvalidTradeException.class, () -> tradeService.saveTrade(dto));
    }

    @Test
    void shouldRejectTradeWithLowerVersion() {
        UUID tradeId = UUID.randomUUID();
        Trade existing = new Trade(tradeId, 5, "CP-1", "B1", LocalDate.now().plusDays(10), LocalDate.now(), false);

        when(tradeRepository.findById(tradeId)).thenReturn(Optional.of(existing));

        TradeDto dto = TradeDto.builder()
                .tradeId(tradeId)
                .version(3)
                .bookId("B1")
                .counterPartyId("CP-1")
                .maturityDate(LocalDate.now().plusDays(5))
                .build();

        assertThrows(InvalidTradeException.class, () -> tradeService.saveTrade(dto));
    }

    @Test
    void shouldSaveNewTrade() {
        TradeDto dto = TradeDto.builder()
                .tradeId(UUID.randomUUID())
                .version(1)
                .bookId("B1")
                .counterPartyId("CP-1")
                .maturityDate(LocalDate.now().plusDays(5))
                .build();

        when(tradeRepository.findById(dto.getTradeId())).thenReturn(Optional.empty());
        when(tradeRepository.save(any(Trade.class))).thenAnswer(i -> i.getArgument(0));

        Trade result = tradeService.saveTrade(dto);

        assertNotNull(result);
        assertEquals(dto.getBookId(), result.getBookId());
        verify(tradeRepository).save(any(Trade.class));
        verify(tradeHistoryRepository).save(any(TradeHistory.class));
    }

    @Test
    void shouldUpdateExistingTradeWithSameVersion() {
        UUID tradeId = UUID.randomUUID();
        Trade existing = new Trade(tradeId, 1, "B1", "CP-1", LocalDate.now().plusDays(5), LocalDate.now(), false);

        TradeDto dto = TradeDto.builder()
                .tradeId(tradeId)
                .version(1)
                .bookId("B2")
                .counterPartyId("CP-2")
                .maturityDate(LocalDate.now().plusDays(10))
                .build();

        when(tradeRepository.findById(tradeId)).thenReturn(Optional.of(existing));
        when(tradeRepository.save(any(Trade.class))).thenAnswer(i -> i.getArgument(0));

        Trade result = tradeService.saveTrade(dto);

        assertEquals("B2", result.getBookId());
        assertEquals("CP-2", result.getCounterPartyId());
        verify(tradeHistoryRepository).save(any(TradeHistory.class));
    }

    @Test
    void shouldUpdateExistingTradeWithHigherVersion() {
        UUID tradeId = UUID.randomUUID();
        Trade existing = new Trade(tradeId, 1, "B1", "CP-1", LocalDate.now().plusDays(5), LocalDate.now(), false);

        TradeDto dto = TradeDto.builder()
                .tradeId(tradeId)
                .version(2)
                .bookId("B2")
                .counterPartyId("CP-2")
                .maturityDate(LocalDate.now().plusDays(10))
                .build();

        when(tradeRepository.findById(tradeId)).thenReturn(Optional.of(existing));
        when(tradeRepository.save(any(Trade.class))).thenAnswer(i -> i.getArgument(0));

        Trade result = tradeService.saveTrade(dto);

        assertEquals(2, result.getVersion());
        assertEquals("B2", result.getBookId());
        verify(tradeHistoryRepository).save(any(TradeHistory.class));
    }

    @Test
    void getAllTrades_shouldReturnList() {
        Trade t1 = new Trade();
        Trade t2 = new Trade();
        when(tradeRepository.findAll()).thenReturn(Arrays.asList(t1, t2));

        List<Trade> result = tradeService.getAllTrades();

        assertEquals(2, result.size());
        verify(tradeRepository).findAll();
    }

    @Test
    void markExpiredTrades_shouldMarkAndSave() {
        Trade t1 = new Trade();
        t1.setMaturityDate(LocalDate.now().minusDays(1));
        t1.setExpired(false);

        Trade t2 = new Trade();
        t2.setMaturityDate(LocalDate.now().minusDays(2));
        t2.setExpired(false);

        when(tradeRepository.findByExpiredFalseAndMaturityDateBefore(LocalDate.now()))
                .thenReturn(Arrays.asList(t1, t2));

        tradeService.markExpiredTrades();

        assertTrue(t1.isExpired());
        assertTrue(t2.isExpired());
        verify(tradeRepository).saveAll(Arrays.asList(t1, t2));
    }

    @Test
    void markExpiredTrades_shouldDoNothingIfNoExpiredTrades() {
        LocalDate d = LocalDate.now();
        doReturn(List.of())
                .when(tradeRepository)
                .findByExpiredFalseAndMaturityDateBefore(d);

        tradeService.markExpiredTrades();

        verify(tradeRepository, never()).saveAll(any());
    }
}
