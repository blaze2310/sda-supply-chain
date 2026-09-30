db = db.getSiblingDB("supply_chain_db");

db.createCollection("orders");
db.createCollection("inventory");
db.createCollection("retail_sales");
db.createCollection("manufacturing");
db.createCollection("logistics");
db.createCollection("customer_activity");
db.createCollection("weather");

db.orders.createIndex(
    { event_id: 1 },
    { unique: true }
);

db.inventory.createIndex(
    { event_id: 1 },
    { unique: true }
);

db.retail_sales.createIndex(
    { event_id: 1 },
    { unique: true }
);

db.manufacturing.createIndex(
    { event_id: 1 },
    { unique: true }
);

db.logistics.createIndex(
    { event_id: 1 },
    { unique: true }
);

db.customer_activity.createIndex(
    { event_id: 1 },
    { unique: true }
);

db.weather.createIndex(
    { event_id: 1 },
    { unique: true }
);


/* Cross-dataset lookup indexes */

db.orders.createIndex({ "payload.order_id": 1 });
db.orders.createIndex({ "payload.order_item_id": 1 });
db.orders.createIndex({ "payload.customer_id": 1 });
db.orders.createIndex({ "payload.product_id": 1 });
db.orders.createIndex({ "payload.location_id": 1 });

db.inventory.createIndex({ "payload.product_id": 1 });
db.inventory.createIndex({ "payload.location_id": 1 });
db.inventory.createIndex({ "payload.store_id": 1 });

db.retail_sales.createIndex({ "payload.product_id": 1 });
db.retail_sales.createIndex({ "payload.location_id": 1 });
db.retail_sales.createIndex({ "payload.store_id": 1 });

db.manufacturing.createIndex({ "payload.product_id": 1 });
db.manufacturing.createIndex({ "payload.facility_id": 1 });

db.logistics.createIndex({ "payload.order_id": 1 });
db.logistics.createIndex({ "payload.order_item_id": 1 });
db.logistics.createIndex({ "payload.customer_id": 1 });
db.logistics.createIndex({ "payload.product_id": 1 });
db.logistics.createIndex({ "payload.location_id": 1 });

db.customer_activity.createIndex({ "payload.customer_id": 1 });
db.customer_activity.createIndex({ "payload.product_id": 1 });
db.customer_activity.createIndex({ "payload.order_id": 1 });
db.customer_activity.createIndex({ "payload.location_id": 1 });
db.customer_activity.createIndex({ "payload.session_id": 1 });

db.weather.createIndex({ "payload.location_id": 1 });
db.weather.createIndex({ "payload.observation_time": 1 });

print("supply_chain_db initialized successfully");