package com.nishanthr.pipeline.strategy;

import com.nishanthr.pipeline.model.OrderEvent;

/**
 * Core strategy interface for channel-specific order processing.
 *
 * Each implementation handles enrichment, validation, and processing
 * logic for a specific order type/channel combination.
 *
 * Implementations are auto-registered into the EventProcessorRegistry
 * via Spring — no factory modifications needed when adding new channels.
 */
public interface OrderProcessingStrategy {

    /**
     * The composite key this strategy handles.
     * Format: "ORDER_TYPE/CHANNEL" e.g. "STAPLESTOCK/US"
     */
    String getStrategyKey();

    /**
     * Enrich the order event with item details, location, and supplier data.
     */
    OrderEvent enrich(OrderEvent event);

    /**
     * Validate the enriched order event.
     * Throws OrderValidationException if validation fails.
     */
    void validate(OrderEvent event);

    /**
     * Process the enriched + validated order event.
     * Returns the processed order ID.
     */
    String process(OrderEvent event);
}
