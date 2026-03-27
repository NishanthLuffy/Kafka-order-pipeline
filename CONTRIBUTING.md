# Contributing

## Adding a new order channel

This is the most common extension point. Adding a new channel takes about 10 minutes:

1. Create a new class in `strategy/impl/` implementing `OrderProcessingStrategy`
2. Annotate it with `@Component`
3. Set `getStrategyKey()` to return your composite key e.g. `"CROSSDOCK/CA"`
4. Implement `enrich()`, `validate()`, and `process()` with your channel-specific logic
5. That's it — the `EventProcessorRegistry` picks it up automatically at startup

No changes to the registry, consumer, or any existing class needed.

## Running locally

```bash
docker-compose up -d
mvn spring-boot:run
```

## Running tests

```bash
mvn test
```

## Code style

- Java 17, Spring Boot 3
- Slf4j for logging — no `System.out.println`
- Each strategy handles one channel — keep them focused and small
- Add a unit test for any new strategy's validation logic
