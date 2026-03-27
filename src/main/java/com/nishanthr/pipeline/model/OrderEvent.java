package com.nishanthr.pipeline.model;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class OrderEvent {

    private String eventId;
    private String orderId;
    private String orderType;   // STAPLESTOCK, DSD, CROSSDOCK
    private String channel;     // composite key part 2
    private List<OrderItem> items;
    private Instant createdAt;

    public OrderEvent() {
        this.eventId = UUID.randomUUID().toString();
        this.createdAt = Instant.now();
    }

    public OrderEvent(String orderId, String orderType, String channel, List<OrderItem> items) {
        this();
        this.orderId = orderId;
        this.orderType = orderType;
        this.channel = channel;
        this.items = items;
    }

    // composite key used by strategy registry
    public String getStrategyKey() {
        return orderType + "/" + channel;
    }

    public String getEventId() { return eventId; }
    public String getOrderId() { return orderId; }
    public String getOrderType() { return orderType; }
    public String getChannel() { return channel; }
    public List<OrderItem> getItems() { return items; }
    public Instant getCreatedAt() { return createdAt; }

    public void setEventId(String eventId) { this.eventId = eventId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    public void setOrderType(String orderType) { this.orderType = orderType; }
    public void setChannel(String channel) { this.channel = channel; }
    public void setItems(List<OrderItem> items) { this.items = items; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
}
