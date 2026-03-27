package com.nishanthr.pipeline.enrichment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Enrichment service with Redis caching.
 *
 * Supplier and location data changes infrequently — the same itemId
 * appears in thousands of orders daily. Without caching, every item
 * on every order hits the MDM service or DB.
 *
 * With Redis:
 *   - First lookup → hits MDM/DB → cached in Redis for 60 minutes
 *   - All subsequent lookups → served from Redis in <1ms
 *   - Cache miss rate drops to near zero for high-frequency items
 */
@Service
@EnableCaching
public class EnrichmentService {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentService.class);

    // Stub data — replace with real MDM GraphQL calls in production
    private static final Map<String, String> ITEM_SUPPLIER_MAP = Map.of(
            "ITEM-123", "SUPPLIER-A",
            "ITEM-456", "SUPPLIER-B",
            "ITEM-789", "SUPPLIER-C"
    );

    private static final Map<String, String> ITEM_LOCATION_MAP = Map.of(
            "ITEM-123", "DC-001",
            "ITEM-456", "DC-002",
            "ITEM-789", "DC-001"
    );

    /**
     * Resolves supplier for an item.
     * Result cached in Redis under key "supplier::{itemId}".
     * Cache TTL is 60 minutes — controlled by RedisCacheManager.
     *
     * @Cacheable means: check Redis first, only call this method
     * if no cached value exists for this key.
     */
    @Cacheable(value = "supplier", key = "#itemId")
    public String resolveSupplier(String itemId) {
        log.debug("Cache MISS — resolving supplier for item: {}", itemId);
        // In production: call Item MDM GraphQL API here
        String supplierId = ITEM_SUPPLIER_MAP.get(itemId);
        if (supplierId == null) {
            log.warn("No supplier found for item: {}", itemId);
        }
        return supplierId;
    }

    /**
     * Resolves distribution center location for an item + channel.
     * Cached under key "location::{itemId}::{channel}".
     *
     * Channel is part of the key because the same item can ship
     * from different DCs depending on the channel (US vs CA etc.)
     */
    @Cacheable(value = "location", key = "#itemId + '::' + #channel")
    public String resolveLocation(String itemId, String channel) {
        log.debug("Cache MISS — resolving location for item: {}, channel: {}", itemId, channel);
        // In production: call Location service API here
        String locationId = ITEM_LOCATION_MAP.get(itemId);
        log.debug("Resolved location [item={}, channel={}, location={}]",
                itemId, channel, locationId);
        return locationId;
    }
}