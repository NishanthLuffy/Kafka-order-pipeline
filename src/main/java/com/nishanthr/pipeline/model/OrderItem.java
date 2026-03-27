package com.nishanthr.pipeline.model;

public class OrderItem {

    private String itemId;
    private int quantity;
    private double unitCost;
    private String supplierId;      // populated during enrichment
    private String locationId;      // populated during enrichment

    public OrderItem() {}

    public OrderItem(String itemId, int quantity, double unitCost) {
        this.itemId = itemId;
        this.quantity = quantity;
        this.unitCost = unitCost;
    }

    public double getTotalCost() {
        return quantity * unitCost;
    }

    public String getItemId() { return itemId; }
    public int getQuantity() { return quantity; }
    public double getUnitCost() { return unitCost; }
    public String getSupplierId() { return supplierId; }
    public String getLocationId() { return locationId; }

    public void setItemId(String itemId) { this.itemId = itemId; }
    public void setQuantity(int quantity) { this.quantity = quantity; }
    public void setUnitCost(double unitCost) { this.unitCost = unitCost; }
    public void setSupplierId(String supplierId) { this.supplierId = supplierId; }
    public void setLocationId(String locationId) { this.locationId = locationId; }
}
