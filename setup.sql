CREATE TABLE IF NOT EXISTS dim_item (
  item_id INTEGER PRIMARY KEY,   
  item_name varchar(32),          
  item_ppu NUMERIC
);

CREATE TABLE IF NOT EXISTS dim_payment (
  payment_type_id INTEGER PRIMARY KEY,   
  payment_type varchar(32)          
);

CREATE TABLE IF NOT EXISTS dim_location (
  location_type_id INTEGER PRIMARY KEY,   
  location_type varchar(32)          
);

CREATE TABLE IF NOT EXISTS fact_orders (
  transaction_id char(11),
  item_id INTEGER,        
  item_quantity INTEGER,               
  item_total_price NUMERIC, 
  payment_type_id INTEGER,        
  location_type_id INTEGER,    
  transaction_Date DATE,
  PRIMARY KEY (transaction_id),
  FOREIGN KEY (payment_type_id) REFERENCES dim_payment(payment_type_id),
  FOREIGN KEY (location_type_id) REFERENCES dim_location(location_type_id),
  FOREIGN KEY (item_id) REFERENCES dim_item(item_id)
);

CREATE TABLE IF NOT EXISTS fact_orders_temp (
  transaction_id char(11),
  item_id INTEGER,        
  item_quantity INTEGER,               
  item_total_price NUMERIC, 
  payment_type_id INTEGER,        
  location_type_id INTEGER,    
  transaction_Date DATE
);