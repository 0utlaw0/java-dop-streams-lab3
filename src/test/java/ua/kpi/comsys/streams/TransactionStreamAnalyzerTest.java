package ua.kpi.comsys.streams;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class TransactionStreamAnalyzerTest {

    private TransactionStreamAnalyzer analyzer;

    @BeforeEach
    void setUp() {
        analyzer = new TransactionStreamAnalyzer();
    }

    private Stream<Transaction> infiniteIncrementalStream() {
        AtomicInteger counter = new AtomicInteger(1);
        return Stream.generate(() -> {
            int id = counter.getAndIncrement();
            return new Transaction(
                    "TX-" + id,
                    id % 2 == 0 ? "ELECTRONICS" : "GROCERIES",
                    BigDecimal.valueOf(id * 10L),
                    LocalDateTime.now()
            );
        });
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    @DisplayName("findFirstHighValueTransaction: знаходить першу транзакцію > threshold у нескінченному стрімі")
    void findFirstHighValueTransaction_infiniteStream_findsMatch() {
        Optional<Transaction> result = analyzer.findFirstHighValueTransaction(
                infiniteIncrementalStream(),
                BigDecimal.valueOf(45)
        );

        assertTrue(result.isPresent());
        assertEquals("TX-5", result.get().transactionId());
        assertEquals(BigDecimal.valueOf(50), result.get().amount());
    }

    @Test
    @DisplayName("findFirstHighValueTransaction: повертає Optional.empty(), якщо збігів немає у скінченному стрімі")
    void findFirstHighValueTransaction_finiteStreamNoMatch_returnsEmpty() {
        Stream<Transaction> finiteStream = Stream.of(
                new Transaction("TX-1", "BOOKS", BigDecimal.TEN, LocalDateTime.now()),
                new Transaction("TX-2", "BOOKS", BigDecimal.valueOf(20), LocalDateTime.now())
        );

        Optional<Transaction> result = analyzer.findFirstHighValueTransaction(
                finiteStream,
                BigDecimal.valueOf(100)
        );

        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("findFirstHighValueTransaction: кидає IllegalArgumentException при null аргументах")
    void findFirstHighValueTransaction_nullArguments_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.findFirstHighValueTransaction(null, BigDecimal.TEN));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.findFirstHighValueTransaction(Stream.empty(), null));
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    @DisplayName("takeTransactionsUntilBudgetExceeded: коректно зупиняється на нескінченному стрімі при перевищенні бюджету")
    void takeTransactionsUntilBudgetExceeded_infiniteStream_stopsBeforeExceeding() {
        // Суми: 10, 20, 30, 40...
        // 10 + 20 + 30 = 60 <= 65; наступна +40 = 100 > 65 -> зупинка
        List<Transaction> result = analyzer.takeTransactionsUntilBudgetExceeded(
                infiniteIncrementalStream(),
                BigDecimal.valueOf(65)
        );

        assertEquals(3, result.size());
        assertEquals("TX-1", result.get(0).transactionId());
        assertEquals("TX-2", result.get(1).transactionId());
        assertEquals("TX-3", result.get(2).transactionId());
    }

    @Test
    @DisplayName("takeTransactionsUntilBudgetExceeded: кидає IllegalArgumentException при null або від'ємному бюджеті")
    void takeTransactionsUntilBudgetExceeded_invalidArguments_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.takeTransactionsUntilBudgetExceeded(null, BigDecimal.TEN));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.takeTransactionsUntilBudgetExceeded(Stream.empty(), null));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.takeTransactionsUntilBudgetExceeded(Stream.empty(), BigDecimal.valueOf(-1)));
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    @DisplayName("processBatches: формує рівно maxBatches батчів по batchSize елементів із нескінченного стріму")
    void processBatches_infiniteStream_stopsAfterMaxBatches() {
        List<List<Transaction>> batches = analyzer.processBatches(
                infiniteIncrementalStream(),
                3,
                2
        );

        assertEquals(2, batches.size());
        assertEquals(3, batches.get(0).size());
        assertEquals(3, batches.get(1).size());

        assertEquals("TX-1", batches.get(0).get(0).transactionId());
        assertEquals("TX-3", batches.get(0).get(2).transactionId());
        assertEquals("TX-4", batches.get(1).get(0).transactionId());
        assertEquals("TX-6", batches.get(1).get(2).transactionId());
    }

    @Test
    @DisplayName("processBatches: повертає неповний останній батч, якщо скінченний стрім закінчився раніше")
    void processBatches_finiteStreamPartialBatch_flushesRemaining() {
        Stream<Transaction> finiteStream = infiniteIncrementalStream().limit(5);

        List<List<Transaction>> batches = analyzer.processBatches(finiteStream, 3, 4);

        assertEquals(2, batches.size());
        assertEquals(3, batches.get(0).size());
        assertEquals(2, batches.get(1).size());
    }

    @Test
    @DisplayName("processBatches: кидає IllegalArgumentException при невалідних параметрах")
    void processBatches_invalidArguments_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.processBatches(null, 2, 2));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.processBatches(Stream.empty(), 0, 2));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.processBatches(Stream.empty(), 2, -5));
    }
}