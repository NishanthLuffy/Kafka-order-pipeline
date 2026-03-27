package com.nishanthr.pipeline.enrichment;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Enrichment service — resolves item metadata from upstream sources.
 *
 * In production, these calls would hit Item MDM, Lead Time,
 * and Flow Policy GraphQL endpoints. Here they are stubbed
 * with realistic in-memory data for local development.
 */
@Service
public class EnrichmentService {

//    private static final com.nishanthr.pipeline.enrichment.Logger log = LoggerFactory.getLogger(EnrichmentService.class);

    // Stub supplier mappings — replace with real MDM calls
    private static final Map<String, String> ITEM_SUPPLIER_MAP = Map.of(
            "ITEM-123", "SUPPLIER-A",
            "ITEM-456", "SUPPLIER-B",
            "ITEM-789", "SUPPLIER-C"
    );

    // Stub location mappings — replace with real location service calls
    private static final Map<String, String> ITEM_LOCATION_MAP = Map.of(
            "ITEM-123", "DC-001",
            "ITEM-456", "DC-002",
            "ITEM-789", "DC-001"
    );
    private static final Logger log = LoggerFactory.getLogger(EnrichmentService.class);

    public String resolveSupplier(String itemId) {
        String supplierId = ITEM_SUPPLIER_MAP.get(itemId);
        if (supplierId == null) {
            log.info("No supplier found for item: {}", itemId);
        }
        return supplierId;
    }

    public String resolveLocation(String itemId, String channel) {
        String locationId = ITEM_LOCATION_MAP.get(itemId);
       log.info("Resolved location for item {} in channel {}: {}", itemId, channel, locationId);
        return locationId;
    }
}
