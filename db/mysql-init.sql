CREATE DATABASE IF NOT EXISTS supply_chain_db;
USE supply_chain_db;


-- =========================================================
-- 1. ORDERS
-- =========================================================

CREATE TABLE IF NOT EXISTS orders (
    event_id VARCHAR(100) PRIMARY KEY,
    entity_id VARCHAR(100),

    order_item_id VARCHAR(100),
    order_id VARCHAR(100),
    customer_id VARCHAR(100),
    product_id VARCHAR(150),
    location_id VARCHAR(100),

    order_date DATETIME,
    ship_date DATETIME,
    ship_mode VARCHAR(100),

    customer_name VARCHAR(255),
    segment VARCHAR(100),

    city VARCHAR(150),
    state VARCHAR(150),
    country VARCHAR(150),
    market VARCHAR(100),
    region VARCHAR(150),

    category VARCHAR(150),
    sub_category VARCHAR(150),
    product_name TEXT,

    sales DECIMAL(18,4),
    quantity INT,
    discount DECIMAL(12,6),
    profit DECIMAL(18,4),
    shipping_cost DECIMAL(18,4),

    order_priority VARCHAR(100),

    sales_per_unit DECIMAL(18,4),
    profit_margin_pct DECIMAL(18,6),
    profit_status VARCHAR(100),
    dispatch_lead_days INT,

    currency_code VARCHAR(20),

    latitude DECIMAL(11,7),
    longitude DECIMAL(11,7),

    source_system VARCHAR(100),
    event_type VARCHAR(100),

    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_orders_order_id (order_id),
    INDEX idx_orders_order_item_id (order_item_id),
    INDEX idx_orders_customer_id (customer_id),
    INDEX idx_orders_product_id (product_id),
    INDEX idx_orders_location_id (location_id)
);


-- =========================================================
-- 2. INVENTORY
-- =========================================================

CREATE TABLE IF NOT EXISTS inventory (
    event_id VARCHAR(100) PRIMARY KEY,
    entity_id VARCHAR(100),

    inventory_record_id VARCHAR(100),
    product_id VARCHAR(150),
    store_id VARCHAR(100),
    location_id VARCHAR(100),

    inventory_date DATETIME,
    price_status VARCHAR(100),

    supplier_id VARCHAR(100),

    product_name TEXT,
    category VARCHAR(150),
    sub_category VARCHAR(150),

    quantity_on_hand INT,

    stock_retail_value DECIMAL(18,4),
    unit_selling_price DECIMAL(18,4),
    unit_cost DECIMAL(18,4),

    inventory_status VARCHAR(100),
    inventory_value DECIMAL(18,4),
    shortage_quantity DECIMAL(18,4),

    city VARCHAR(150),
    state VARCHAR(150),
    country VARCHAR(150),
    market VARCHAR(100),
    region VARCHAR(150),

    latitude DECIMAL(11,7),
    longitude DECIMAL(11,7),

    currency_code VARCHAR(20),

    source_system VARCHAR(100),
    event_type VARCHAR(100),

    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_inventory_record_id (inventory_record_id),
    INDEX idx_inventory_product_id (product_id),
    INDEX idx_inventory_store_id (store_id),
    INDEX idx_inventory_location_id (location_id)
);


-- =========================================================
-- 3. RETAIL SALES
-- =========================================================

CREATE TABLE IF NOT EXISTS retail_sales (
    event_id VARCHAR(100) PRIMARY KEY,
    entity_id VARCHAR(100),

    sales_record_id VARCHAR(100),
    product_id VARCHAR(150),
    store_id VARCHAR(100),
    location_id VARCHAR(100),

    transaction_date DATETIME,

    price_status VARCHAR(100),
    is_return VARCHAR(50),

    supplier_id VARCHAR(100),

    product_name TEXT,
    category VARCHAR(150),
    sub_category VARCHAR(150),

    quantity_sold DECIMAL(18,4),
    sales_amount DECIMAL(18,4),
    cost_of_goods_sold DECIMAL(18,4),
    transaction_count INT,

    return_status VARCHAR(100),
    gross_profit DECIMAL(18,4),

    city VARCHAR(150),
    state VARCHAR(150),
    country VARCHAR(150),
    market VARCHAR(100),
    region VARCHAR(150),

    latitude DECIMAL(11,7),
    longitude DECIMAL(11,7),

    currency_code VARCHAR(20),

    units_sold DECIMAL(18,4),
    units_returned DECIMAL(18,4),

    source_system VARCHAR(100),
    event_type VARCHAR(100),

    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_sales_record_id (sales_record_id),
    INDEX idx_sales_product_id (product_id),
    INDEX idx_sales_store_id (store_id),
    INDEX idx_sales_location_id (location_id)
);


-- =========================================================
-- 4. MANUFACTURING
-- =========================================================

CREATE TABLE IF NOT EXISTS manufacturing (
    event_id VARCHAR(100) PRIMARY KEY,
    entity_id VARCHAR(100),

    manufacturing_record_id VARCHAR(100),

    production_timestamp DATETIME,

    material_category VARCHAR(150),
    material_name VARCHAR(255),

    quantity_used_kg DECIMAL(18,4),
    energy_consumption_kwh DECIMAL(18,4),
    production_output_units DECIMAL(18,4),

    machine_id VARCHAR(100),
    material_id VARCHAR(100),
    production_batch_id VARCHAR(100),

    recycled_material_rate DECIMAL(18,6),
    defect_rate DECIMAL(18,6),
    defect_units DECIMAL(18,4),
    good_output_units DECIMAL(18,4),

    material_efficiency_units_per_kg DECIMAL(18,6),
    energy_per_good_unit_kwh DECIMAL(18,6),

    product_id VARCHAR(150),
    product_name TEXT,
    category VARCHAR(150),
    sub_category VARCHAR(150),

    facility_id VARCHAR(100),

    city VARCHAR(150),
    state VARCHAR(150),
    country VARCHAR(150),
    market VARCHAR(100),
    region VARCHAR(150),

    latitude DECIMAL(11,7),
    longitude DECIMAL(11,7),

    source_system VARCHAR(100),
    event_type VARCHAR(100),

    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_mfg_record_id (manufacturing_record_id),
    INDEX idx_mfg_product_id (product_id),
    INDEX idx_mfg_facility_id (facility_id),
    INDEX idx_mfg_batch_id (production_batch_id)
);


-- =========================================================
-- 5. LOGISTICS
-- =========================================================

CREATE TABLE IF NOT EXISTS logistics (
    event_id VARCHAR(100) PRIMARY KEY,
    entity_id VARCHAR(100),

    shipment_id VARCHAR(100),

    order_item_id VARCHAR(100),
    order_id VARCHAR(100),

    order_date DATETIME,
    ship_date DATETIME,

    shipment_mode VARCHAR(100),
    service_level VARCHAR(100),

    warehouse_id VARCHAR(100),
    origin_facility_id VARCHAR(100),

    origin_city VARCHAR(150),
    origin_state VARCHAR(150),
    origin_country VARCHAR(150),
    origin_market VARCHAR(100),
    origin_region VARCHAR(150),

    origin_latitude DECIMAL(11,7),
    origin_longitude DECIMAL(11,7),

    customer_id VARCHAR(100),
    location_id VARCHAR(100),

    destination_city VARCHAR(150),
    destination_state VARCHAR(150),
    destination_country VARCHAR(150),
    destination_market VARCHAR(100),
    destination_region VARCHAR(150),

    destination_latitude DECIMAL(11,7),
    destination_longitude DECIMAL(11,7),

    product_id VARCHAR(150),
    product_name TEXT,
    category VARCHAR(150),
    sub_category VARCHAR(150),

    customer_care_calls INT,
    customer_rating DECIMAL(10,4),
    prior_purchases INT,

    product_importance VARCHAR(100),
    customer_gender VARCHAR(50),

    product_cost DECIMAL(18,4),
    discount_rate DECIMAL(18,6),
    weight_g DECIMAL(18,4),

    delivery_status VARCHAR(100),
    on_time_flag VARCHAR(50),

    order_priority VARCHAR(100),
    shipping_cost DECIMAL(18,4),
    dispatch_lead_days INT,

    currency_code VARCHAR(20),

    source_system VARCHAR(100),
    event_type VARCHAR(100),

    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_logistics_shipment_id (shipment_id),
    INDEX idx_logistics_order_id (order_id),
    INDEX idx_logistics_order_item_id (order_item_id),
    INDEX idx_logistics_customer_id (customer_id),
    INDEX idx_logistics_product_id (product_id),
    INDEX idx_logistics_location_id (location_id)
);


-- =========================================================
-- 6. CUSTOMER ACTIVITY
-- =========================================================

CREATE TABLE IF NOT EXISTS customer_activity (
    event_id VARCHAR(100) PRIMARY KEY,
    entity_id VARCHAR(100),

    session_id VARCHAR(100),

    customer_id VARCHAR(100),
    product_id VARCHAR(150),

    order_id VARCHAR(100),
    order_item_id VARCHAR(100),
    location_id VARCHAR(100),

    customer_name VARCHAR(255),
    segment VARCHAR(100),

    product_name TEXT,

    activity_type VARCHAR(100),

    quantity DECIMAL(18,4),
    cart_value DECIMAL(18,4),

    city VARCHAR(150),
    state VARCHAR(150),
    country VARCHAR(150),
    market VARCHAR(100),
    region VARCHAR(150),

    latitude DECIMAL(11,7),
    longitude DECIMAL(11,7),

    event_timestamp DATETIME,

    source_system VARCHAR(100),
    event_type VARCHAR(100),

    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_activity_session_id (session_id),
    INDEX idx_activity_customer_id (customer_id),
    INDEX idx_activity_product_id (product_id),
    INDEX idx_activity_order_id (order_id),
    INDEX idx_activity_location_id (location_id)
);


-- =========================================================
-- 7. WEATHER
-- =========================================================

CREATE TABLE IF NOT EXISTS weather (
    event_id VARCHAR(100) PRIMARY KEY,
    entity_id VARCHAR(100),

    location_id VARCHAR(100),

    city VARCHAR(150),
    state VARCHAR(150),
    country VARCHAR(150),
    market VARCHAR(100),
    region VARCHAR(150),

    latitude DECIMAL(11,7),
    longitude DECIMAL(11,7),

    observation_time DATETIME,

    temperature_2m DECIMAL(10,4),
    relative_humidity_2m DECIMAL(10,4),
    precipitation DECIMAL(18,6),
    weather_code INT,
    wind_speed_10m DECIMAL(18,4),
    wind_direction_10m DECIMAL(18,4),

    source_system VARCHAR(100),
    event_type VARCHAR(100),

    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_weather_location_id (location_id),
    INDEX idx_weather_observation_time (observation_time)
);