CREATE MATERIALIZED VIEW
  `terpel-pruebas-369613.DataPath_ACCS.bucket_orderdetails_accs` OPTIONS (enable_refresh = TRUE,
    refresh_interval_minutes = 720,
    max_staleness = INTERVAL "8:0:0" HOUR TO SECOND) 
#CLUSTER BY orderNumber 
AS (
  SELECT
    DISTINCT SAFE_CAST (orderNumber AS INTEGER) AS orderNumber,
    SAFE_CAST(productCode AS STRING) AS productCode,
    SAFE_CAST (quantityOrdered AS INTEGER) AS quantityOrdered,
    SAFE_CAST (priceEach AS NUMERIC) AS priceEach,
    SAFE_CAST (orderLineNumber AS INTEGER) AS orderLineNumber
  FROM
    `terpel-pruebas-369613.DataPath_QTY.bucket_orderdetails_qty` );
#######################################################################################################
CREATE MATERIALIZED VIEW
  `terpel-pruebas-369613.DataPath_ACCS.my_sql_customers_acss` OPTIONS (enable_refresh = TRUE,
    refresh_interval_minutes = 720,
    max_staleness = INTERVAL "8:0:0" HOUR TO SECOND) 
#CLUSTER BY orderNumber 
AS (
  SELECT
    DISTINCT SAFE_CAST (customerNumber AS INTEGER) AS customerNumber,
    SAFE_CAST(customerName AS STRING) AS customerName,
    SAFE_CAST (contactLastName AS STRING) AS contactLastName,
    SAFE_CAST (contactFirstName AS STRING) AS contactFirstName,
    SAFE_CAST (phone AS STRING) AS phone,
    SAFE_CAST (addressLine1 AS STRING) AS addressLine1,
    SAFE_CAST (addressLine2 AS STRING) AS addressLine2,
    SAFE_CAST (city AS STRING) AS city,
    SAFE_CAST (state AS STRING) AS state,
    SAFE_CAST (postalCode AS STRING) AS postalCode,
    SAFE_CAST (country AS STRING) AS country,
    SAFE_CAST (salesRepEmployeeNumber AS INTEGER) AS salesRepEmployeeNumber,
    SAFE_CAST (creditLimit AS NUMERIC) AS creditLimit
  FROM
    `terpel-pruebas-369613.DataPath_QTY.my_sql_customers_qty` );
#######################################################################################################
CREATE MATERIALIZED VIEW
  `terpel-pruebas-369613.DataPath_ACCS.my_sql_employees_accs` OPTIONS (enable_refresh = TRUE,
    refresh_interval_minutes = 720,
    max_staleness = INTERVAL "8:0:0" HOUR TO SECOND) 
#CLUSTER BY orderNumber 
AS (
  SELECT
    DISTINCT SAFE_CAST (employeeNumber AS INTEGER) AS employeeNumber,
    SAFE_CAST(lastName AS STRING) AS lastName,
    SAFE_CAST (firstName AS STRING) AS firstName,
    SAFE_CAST (extension AS STRING) AS extension,
    SAFE_CAST (email AS STRING) AS email,
    SAFE_CAST (officeCode AS INTEGER) AS officeCode,
    SAFE_CAST (reportsTo AS STRING) AS reportsTo,
    SAFE_CAST (jobTitle AS STRING) AS jobTitle
  FROM
    `terpel-pruebas-369613.DataPath_QTY.my_sql_employees_qty` );
#######################################################################################################
CREATE MATERIALIZED VIEW
  `terpel-pruebas-369613.DataPath_ACCS.my_sql_offices_accs` OPTIONS (enable_refresh = TRUE,
    refresh_interval_minutes = 720,
    max_staleness = INTERVAL "8:0:0" HOUR TO SECOND) 
#CLUSTER BY orderNumber 
AS (
  SELECT
    DISTINCT SAFE_CAST (officeCode AS INTEGER) AS officeCode,
    SAFE_CAST(city AS STRING) AS city,
    SAFE_CAST (phone AS STRING) AS phone,
    SAFE_CAST (addressLine1 AS STRING) AS addressLine1,
    SAFE_CAST (addressLine2 AS STRING) AS addressLine2,
    SAFE_CAST (state AS STRING) AS state,
    SAFE_CAST (country AS INTEGER) AS country,
    SAFE_CAST (postalCode AS STRING) AS postalCode,
    SAFE_CAST (territory AS STRING) AS territory,

  FROM
    `terpel-pruebas-369613.DataPath_QTY.my_sql_offices_qty` );
#######################################################################################################
CREATE OR REPLACE MATERIALIZED VIEW
  `terpel-pruebas-369613.DataPath_ACCS.my_sql_orders_accs` OPTIONS (enable_refresh = TRUE,
    refresh_interval_minutes = 720,
    max_staleness = INTERVAL "8:0:0" HOUR TO SECOND) 
#CLUSTER BY orderNumber 
AS (
  SELECT
    DISTINCT SAFE_CAST (orderNumber AS INTEGER) AS orderNumber,
    SAFE_CAST(orderDate AS DATE) AS orderDate,
    SAFE_CAST (requiredDate AS DATE) AS requiredDate,
    SAFE_CAST (shippedDate AS DATE) AS shippedDate,
    SAFE_CAST (status AS STRING) AS status,
    SAFE_CAST (comments AS STRING) AS comments,
    SAFE_CAST (customerNumber AS INTEGER) AS customerNumber

  FROM
    `terpel-pruebas-369613.DataPath_QTY.my_sql_orders_qty` );
#######################################################################################################
CREATE MATERIALIZED VIEW
  `terpel-pruebas-369613.DataPath_ACCS.my_sql_payments_accs` OPTIONS (enable_refresh = TRUE,
    refresh_interval_minutes = 720,
    max_staleness = INTERVAL "8:0:0" HOUR TO SECOND) 
#CLUSTER BY orderNumber 
AS (
  SELECT
    DISTINCT SAFE_CAST (customerNumber AS INTEGER) AS customerNumber,
    SAFE_CAST(checkNumber AS STRING) AS checkNumber,
    SAFE_CAST (paymentDate AS DATE) AS paymentDate,
    SAFE_CAST (amount AS NUMERIC) AS amount

  FROM
    `terpel-pruebas-369613.DataPath_QTY.my_sql_payments_qty` );
#######################################################################################################
CREATE MATERIALIZED VIEW
  `terpel-pruebas-369613.DataPath_ACCS.my_sql_productlines_accs` OPTIONS (enable_refresh = TRUE,
    refresh_interval_minutes = 720,
    max_staleness = INTERVAL "8:0:0" HOUR TO SECOND) 
#CLUSTER BY orderNumber 
AS (
  SELECT
    DISTINCT SAFE_CAST (productLine AS STRING) AS productLine,
    SAFE_CAST(textDescription AS STRING) AS textDescription,
    SAFE_CAST (htmlDescription AS STRING) AS htmlDescription, #DATO VACIO
    SAFE_CAST (image AS STRING) AS image #DATO VACIO

  FROM
    `terpel-pruebas-369613.DataPath_QTY.my_sql_productlines_qty` );
#######################################################################################################
CREATE MATERIALIZED VIEW
  `terpel-pruebas-369613.DataPath_ACCS.my_sql_products_accs` OPTIONS (enable_refresh = TRUE,
    refresh_interval_minutes = 720,
    max_staleness = INTERVAL "8:0:0" HOUR TO SECOND) 
#CLUSTER BY orderNumber 
AS (
  SELECT
    DISTINCT SAFE_CAST (productCode AS STRING) AS productCode,
    SAFE_CAST(productName AS STRING) AS productName,
    SAFE_CAST (productLine AS STRING) AS productLine,
    SAFE_CAST (productScale AS STRING) AS productScale,
    SAFE_CAST (productVendor AS STRING) AS productVendor,
    SAFE_CAST (productDescription AS STRING) AS productDescription,
    SAFE_CAST (quantityInStock AS STRING) AS quantityInStock,
    SAFE_CAST (buyPrice AS STRING) AS buyPrice,
    SAFE_CAST (MSRP AS STRING) AS MSRP
  FROM
    `terpel-pruebas-369613.DataPath_QTY.my_sql_products_qty` );
#######################################################################################################
