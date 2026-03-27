package com.nishanthr.pipeline.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Auto-populating registry for OrderProcessingStrategy implementations.
 *
 * Spring injects all beans implementing OrderProcessingStrategy at startup.
 * The registry maps each strategy to its composite key (orderType/channel).
 *
 * Adding a new channel requires ONLY a new strategy class — zero changes here.
 * This is the same pattern used at Walmart to cut channel onboarding by 80%.
 */
@Component
public class EventProcessorRegistry {

    private static final Logger log = LoggerFactory.getLogger(EventProcessorRegistry.class);

    private final Map<String, OrderProcessingStrategy> registry;

    public EventProcessorRegistry(List<OrderProcessingStrategy> strategies) {
        this.registry = strategies.stream()
                .collect(Collectors.toMap(
                        OrderProcessingStrategy::getStrategyKey,
                        Function.identity()
                ));
        log.info("Registered {} order processing strategies: {}",
                registry.size(), registry.keySet());
    }

    /**
     * Resolve the strategy for a given composite key.
     * Throws if no strategy is registered for the key.
     */
    public OrderProcessingStrategy resolve(String strategyKey) {
        OrderProcessingStrategy strategy = registry.get(strategyKey);
        if (strategy == null) {
            throw new IllegalArgumentException(
                    "No strategy registered for key: " + strategyKey +
                    ". Registered keys: " + registry.keySet()
            );
        }
        return strategy;
    }

    public boolean supports(String strategyKey) {
        return registry.containsKey(strategyKey);
    }
}
