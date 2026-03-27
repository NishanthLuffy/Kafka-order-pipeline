package com.nishanthr.pipeline.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nishanthr.pipeline.model.FailedMessage;
import com.nishanthr.pipeline.model.OrderEvent;
import com.nishanthr.pipeline.persistence.FailedMessageRepository;
import com.nishanthr.pipeline.strategy.EventProcessorRegistry;
import com.nishanthr.pipeline.strategy.OrderProcessingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Service
public class RedriveService {

    private static final Logger log = LoggerFactory.getLogger(RedriveService.class);

    private final FailedMessageRepository failedMessageRepository;
    private final EventProcessorRegistry registry;
    private final ObjectMapper objectMapper;

    public RedriveService(FailedMessageRepository failedMessageRepository,
                           EventProcessorRegistry registry,
                           ObjectMapper objectMapper) {
        this.failedMessageRepository = failedMessageRepository;
        this.registry = registry;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public RedriveResult redriveById(Long id) {
        FailedMessage failedMessage = failedMessageRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException(
                        "No failed message found with id: " + id));
        return redrive(failedMessage);
    }

    @Transactional
    public RedriveSummary redriveAll() {
        List<FailedMessage> pending = failedMessageRepository.findAllPendingRedrive();
        log.info("Starting re-drive of {} failed messages", pending.size());

        int succeeded = 0;
        int failed = 0;

        for (FailedMessage fm : pending) {
            RedriveResult result = redrive(fm);
            if (result.success()) succeeded++;
            else failed++;
        }

        log.info("Re-drive complete. Succeeded={}, Failed={}", succeeded, failed);
        return new RedriveSummary(pending.size(), succeeded, failed);
    }

    private RedriveResult redrive(FailedMessage fm) {
        log.info("Re-driving failed message [id={}, orderId={}, redriveCount={}]",
                fm.getId(), fm.getOrderId(), fm.getRedriveCount());

        fm.setStatus(FailedMessage.Status.REDRIVING);
        fm.setLastRedriveAt(Instant.now());
        fm.setRedriveCount(fm.getRedriveCount() + 1);
        failedMessageRepository.save(fm);

        try {
            OrderEvent event = objectMapper.readValue(fm.getPayload(), OrderEvent.class);
            String strategyKey = event.getStrategyKey();

            if (!registry.supports(strategyKey)) {
                throw new IllegalArgumentException(
                        "No strategy registered for key: " + strategyKey);
            }

            OrderProcessingStrategy strategy = registry.resolve(strategyKey);
            OrderEvent enriched = strategy.enrich(event);
            strategy.validate(enriched);
            String processedId = strategy.process(enriched);

            fm.setStatus(FailedMessage.Status.RESOLVED);
            fm.setResolutionNotes("Re-drive succeeded. ProcessedId=" + processedId);
            failedMessageRepository.save(fm);

            log.info("Re-drive succeeded [id={}, orderId={}, processedId={}]",
                    fm.getId(), fm.getOrderId(), processedId);

            return new RedriveResult(true, fm.getId(), fm.getOrderId(), null);

        } catch (Exception e) {
            fm.setStatus(FailedMessage.Status.DEAD);
            fm.setResolutionNotes("Re-drive failed: " + e.getMessage());
            failedMessageRepository.save(fm);

            log.error("Re-drive failed [id={}, orderId={}]: {}",
                    fm.getId(), fm.getOrderId(), e.getMessage());

            return new RedriveResult(false, fm.getId(), fm.getOrderId(), e.getMessage());
        }
    }

    public record RedriveResult(boolean success, Long id, String orderId, String errorMessage) {}
    public record RedriveSummary(int total, int succeeded, int failed) {}
}