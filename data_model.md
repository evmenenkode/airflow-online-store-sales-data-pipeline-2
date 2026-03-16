# Source Data Model (Raw Data)

# customer_research
Marketing research data about product categories and sales.
| Column      | Description                                  | Type          |
| ----------- | -------------------------------------------- | ------------- |
| date_id     | Research date in format YYYYMMDD             | int           |
| category_id | Product category identifier                  | int           |
| geo_id      | City identifier where research was conducted | int           |
| sales_qty   | Number of items sold                         | bigint        |
| sales_amt   | Sales amount in currency                     | decimal(10,2) |

# user_order_log
User order activity log.
| Column         | Description                       | Type          |
| -------------- | --------------------------------- | ------------- |
| date_time      | Timestamp of order                | timestamp     |
| city_id        | Customer city identifier          | int           |
| city_name      | Customer city name                | varchar(50)   |
| customer_id    | Customer identifier               | int           |
| first_name     | Customer first name               | varchar(50)   |
| last_name      | Customer last name                | varchar(50)   |
| item_id        | Product identifier                | int           |
| item_name      | Product name                      | varchar(50)   |
| quantity       | Number of purchased items         | decimal(10,2) |
| payment_amount | Payment amount                    | decimal(10,2) |
| status         | Order status (shipped / refunded) | varchar(50)   |

# user_activity_log
User activity log.
| Column      | Description            | Type      |
| ----------- | ---------------------- | --------- |
| date_time   | Activity timestamp     | timestamp |
| action_id   | User action identifier | int       |
| customer_id | Customer identifier    | int       |
| quantity    | Number of clicks       | int       |

# price_log
Product price history.
| Column        | Description                            | Type          |
| ------------- | -------------------------------------- | ------------- |
| datetime      | Timestamp when the price became active | timestamp     |
| category_id   | Product category identifier            | int           |
| category_name | Product category name                  | varchar(50)   |
| item_id       | Product identifier                     | int           |
| price         | Product price                          | decimal(10,2) |


# Data Warehouse Model (Mart Layer)
Dimension Tables

# d_customer
Customer dimension.
| Column      | Description              | Type        |
| ----------- | ------------------------ | ----------- |
| customer_id | Customer identifier      | int         |
| first_name  | Customer first name      | varchar(50) |
| last_name   | Customer last name       | varchar(50) |
| city_id     | Customer city identifier | int         |

# d_city
City dimension.
| Column    | Description     | Type        |
| --------- | --------------- | ----------- |
| city_id   | City identifier | int         |
| city_name | City name       | varchar(50) |

# d_item
Product dimension.
| Column      | Description                 | Type        |
| ----------- | --------------------------- | ----------- |
| item_id     | Product identifier          | int         |
| item_name   | Product name                | varchar(50) |
| category_id | Product category identifier | int         |

# d_category
Product category dimension.
| Column        | Description         | Type        |
| ------------- | ------------------- | ----------- |
| category_id   | Category identifier | int         |
| category_name | Category name       | varchar(50) |

# d_calendar
Calendar dimension.
| Column     | Description                | Type       |
| ---------- | -------------------------- | ---------- |
| date_id    | Date identifier (YYYYMMDD) | int        |
| day_num    | Day of month               | tinyint    |
| month_num  | Month number               | tinyint    |
| month_name | Month name                 | varchar(8) |
| year_num   | Year                       | smallint   |


# Fact Tables

# f_activity
User website activity fact table.
| Column       | Description              | Type   |
| ------------ | ------------------------ | ------ |
| activity_id  | Activity identifier      | int    |
| date_id      | Activity date (YYYYMMDD) | int    |
| click_number | Number of clicks         | bigint |

# f_daily_sales
Daily sales fact table.
| Column      | Description                       | Type          |
| ----------- | --------------------------------- | ------------- |
| date_id     | Sales date (YYYYMMDD)             | int           |
| item_id     | Product identifier                | int           |
| customer_id | Customer identifier               | int           |
| price       | Product price                     | decimal(10,2) |
| quantity    | Quantity sold                     | decimal(10,2) |
| amount      | Total purchase amount             | decimal(10,2) |
| status      | Order status (shipped / refunded) | varchar(50)   |

# f_research
Market research fact table.
| Column      | Description                 | Type          |
| ----------- | --------------------------- | ------------- |
| date_id     | Research date (YYYYMMDD)    | int           |
| category_id | Product category identifier | int           |
| geo_id      | City identifier             | int           |
| quantity    | Number of sold items        | decimal(10,2) |
| amount      | Sales amount                | decimal(10,2) |

# f_customer_retention
Customer retention analytical mart aggregated weekly.
| Column                      | Description                                                        |
| --------------------------- | ------------------------------------------------------------------ |
| new_customers_count         | Number of customers who placed exactly one order during the period |
| returning_customers_count   | Number of customers who placed multiple orders during the period   |
| refunded_customer_count     | Number of customers who returned an order                          |
| period_name                 | Aggregation period (weekly)                                        |
| period_id                   | Period identifier (week number)                                    |
| item_id                     | Product category identifier                                        |
| new_customers_revenue       | Revenue generated by new customers                                 |
| returning_customers_revenue | Revenue generated by returning customers                           |
| customers_refunded          | Number of refunds                                                  |


