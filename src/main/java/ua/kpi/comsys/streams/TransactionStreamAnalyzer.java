package ua.kpi.comsys.streams;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Gatherer;
import java.util.stream.Stream;

public class TransactionStreamAnalyzer {


    public Optional<Transaction> findFirstHighValueTransaction(Stream<Transaction> stream, BigDecimal threshold) {
        if (stream == null || threshold == null) {
            throw new IllegalArgumentException("Stream and threshold must not be null");
        }

        return stream
                .filter(Objects::nonNull)
                .filter(tx -> tx.amount() != null && tx.amount().compareTo(threshold) > 0)
                .findFirst();
    }


    public List<Transaction> takeTransactionsUntilBudgetExceeded(Stream<Transaction> stream, BigDecimal maxBudget) {
        if (stream == null || maxBudget == null) {
            throw new IllegalArgumentException("Stream and maxBudget must not be null");
        }
        if (maxBudget.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("maxBudget must not be negative");
        }

        AtomicReference<BigDecimal> accumulated = new AtomicReference<>(BigDecimal.ZERO);

        return stream
                .filter(Objects::nonNull)
                .takeWhile(tx -> {
                    if (tx.amount() == null || tx.amount().compareTo(BigDecimal.ZERO) < 0) {
                        throw new IllegalArgumentException("Transaction amount must be non-null and non-negative");
                    }
                    BigDecimal nextTotal = accumulated.get().add(tx.amount());
                    if (nextTotal.compareTo(maxBudget) <= 0) {
                        accumulated.set(nextTotal);
                        return true;
                    }
                    return false;
                })
                .toList();
    }


    public List<List<Transaction>> processBatches(Stream<Transaction> stream, int batchSize, int maxBatches) {
        if (stream == null) {
            throw new IllegalArgumentException("Stream must not be null");
        }
        if (batchSize <= 0 || maxBatches <= 0) {
            throw new IllegalArgumentException("batchSize and maxBatches must be greater than 0");
        }

        return stream
                .filter(Objects::nonNull)
                .gather(fixedBatchGatherer(batchSize, maxBatches))
                .toList();
    }

    private Gatherer<Transaction, ?, List<Transaction>> fixedBatchGatherer(int batchSize, int maxBatches) {
        class BatchState {
            List<Transaction> currentBatch = new ArrayList<>(batchSize);
            int emittedBatches = 0;
        }

        return Gatherer.ofSequential(
                BatchState::new,
                Gatherer.Integrator.of((state, element, downstream) -> {
                    if (state.emittedBatches >= maxBatches) {
                        return false;
                    }

                    state.currentBatch.add(element);

                    if (state.currentBatch.size() == batchSize) {
                        List<Transaction> completed = List.copyOf(state.currentBatch);
                        state.currentBatch.clear();
                        state.emittedBatches++;

                        boolean downstreamAcceptsMore = downstream.push(completed);
                        return downstreamAcceptsMore && state.emittedBatches < maxBatches;
                    }

                    return true;
                }),
                (state, downstream) -> {
                    if (!state.currentBatch.isEmpty() && state.emittedBatches < maxBatches) {
                        downstream.push(List.copyOf(state.currentBatch));
                    }
                }
        );
    }
}