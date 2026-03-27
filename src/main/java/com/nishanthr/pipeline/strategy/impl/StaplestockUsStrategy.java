package com.nishanthr.pipeline.strategy.impl;

import com.nishanthr.pipeline.enrichment.EnrichmentService;
import com.nishanthr.pipeline.model.OrderEvent;
import com.nishanthr.pipeline.persistence.OrderRepository;
import com.nishanthr.pipeline.strategy.OrderProcessingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Staplestock US channel strategy.
 *
 * Handles enrichment and validation rules specific to
 * staplestock replenishment orders in the US channel.
 */
@Component
public class StaplestockUsStrategy implements OrderProcessingStrategy {

    private static final Logger log = LoggerFactory.getLogger(StaplestockUsStrategy.class);
    private static final String STRATEGY_KEY = "STAPLESTOCK/US";

    private final EnrichmentService enrichmentService;
    private final OrderRepository orderRepository;

    public StaplestockUsStrategy(EnrichmentService enrichmentService,
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
        log.debug("Enriching STAPLESTOCK/US order: {}", event.getOrderId());
        // Enrich each item with supplier and location data
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
        log.debug("Validating STAPLESTOCK/US order: {}", event.getOrderId());
        if (event.getItems() == null || event.getItems().isEmpty()) {
            throw new IllegalArgumentException("Staplestock order must have at least one item");
        }
        event.getItems().forEach(item -> {
            if (item.getSupplierId() == null) {
                throw new IllegalArgumentException(
                        "Item " + item.getItemId() + " could not be resolved to a supplier"
                );
            }
            if (item.getQuantity() <= 0) {
                throw new IllegalArgumentException(
                        "Item " + item.getItemId() + " has invalid quantity: " + item.getQuantity()
                );
            }
        });
    }

    @Override
    public String process(OrderEvent event) {
        log.info("Processing STAPLESTOCK/US order: {} with {} items",
                event.getOrderId(), event.getItems().size());
        // Persist the processed order
        return orderRepository.save(event);
    }
}
