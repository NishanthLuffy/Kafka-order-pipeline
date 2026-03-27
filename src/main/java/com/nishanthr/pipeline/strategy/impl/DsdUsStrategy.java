package com.nishanthr.pipeline.strategy.impl;

import com.nishanthr.pipeline.enrichment.EnrichmentService;
import com.nishanthr.pipeline.model.OrderEvent;
import com.nishanthr.pipeline.persistence.OrderRepository;
import com.nishanthr.pipeline.strategy.OrderProcessingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Direct Store Delivery (DSD) US channel strategy.
 *
 * DSD orders have different validation rules than staplestock —
 * supplier must be pre-approved for direct delivery and
 * cost thresholds apply per order line.
 */
@Component
public class DsdUsStrategy implements OrderProcessingStrategy {

    private static final Logger log = LoggerFactory.getLogger(DsdUsStrategy.class);
    private static final String STRATEGY_KEY = "DSD/US";
    private static final double MAX_LINE_COST = 50_000.00;

    private final EnrichmentService enrichmentService;
    private final OrderRepository orderRepository;

    public DsdUsStrategy(EnrichmentService enrichmentService,
                          OrderRepository orderRepository) {
        this.enrichmentService = enrichmentService;
        this.orderRepository = orderRepository;
    }

    @Override
    public String getStrategyKey() {
        return STRATEGY_KEY;
    }

    @Override
    public OrderEvent enrich(OrderEvent event) {
        log.debug("Enriching DSD/US order: {}", event.getOrderId());
        event.getItems().forEach(item -> {
            String supplierId = enrichmentService.resolveSupplier(item.getItemId());
            String locationId = enrichmentService.resolveLocation(item.getItemId(), event.getChannel());
            item.setSupplierId(supplierId);
            item.setLocationId(locationId);
        });
        return event;
    }

    @Override
    public void validate(OrderEvent event) {
        log.debug("Validating DSD/US order: {}", event.getOrderId());
        if (event.getItems() == null || event.getItems().isEmpty()) {
            throw new IllegalArgumentException("DSD order must have at least one item");
        }
        event.getItems().forEach(item -> {
            if (item.getSupplierId() == null) {
                throw new IllegalArgumentException(
                        "DSD item " + item.getItemId() + " has no resolved supplier"
                );
            }
            // DSD-specific: enforce line cost threshold
            if (item.getTotalCost() > MAX_LINE_COST) {
                throw new IllegalArgumentException(
                        "DSD item " + item.getItemId() + " exceeds max line cost of $" + MAX_LINE_COST
                );
            }
        });
    }

    @Override
    public String process(OrderEvent event) {
        log.info("Processing DSD/US order: {} with {} items",
                event.getOrderId(), event.getItems().size());
        return orderRepository.save(event);
    }
}
