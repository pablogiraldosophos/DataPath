CREATE PROCEDURE
  DataPath_QTY.sp_bucket_orderdetails_qty()
BEGIN
MERGE
  terpel-pruebas-369613.DataPath_QTY.bucket_orderdetails_qty AS a
USING
  (
  SELECT
    orderNumber,
    productCode,
    quantityOrdered,
    priceEach,
    orderLineNumber
  FROM
    terpel-pruebas-369613.DataPath_RAW.bucket_orderdetails_raw) AS b
ON
  a.orderNumber = b.orderNumber
  WHEN NOT MATCHED THEN 
  INSERT (orderNumber, productCode, quantityOrdered, priceEach, orderLineNumber) 
  VALUES (b.orderNumber, b.productCode, b.quantityOrdered, b.priceEach, b.orderLineNumber);
END
###########################################################################################
CREATE PROCEDURE
  DataPath_QTY.sp_my_sql_customers_qty()
BEGIN
MERGE
  terpel-pruebas-369613.DataPath_QTY.my_sql_customers_qty AS a
USING
  (
  SELECT
    customerNumber,
    customerName,
    contactLastName,
    contactFirstName,
    phone,
    addressLine1,
    addressLine2,
    city,
    state,
    postalCode,
    country,
    salesRepEmployeeNumber,
    creditLimit
  FROM
    terpel-pruebas-369613.DataPath_RAW.my_sql_customers_raw) AS b
ON
  a.customerNumber = b.customerNumber
  WHEN NOT MATCHED THEN 
  INSERT ( customerNumber, customerName, contactLastName, contactFirstName, phone, addressLine1, addressLine2, city, state, postalCode, country, salesRepEmployeeNumber, creditLimit) 
  VALUES ( b.customerNumber, b.customerName, b.contactLastName, b.contactFirstName, b.phone, b.addressLine1, b.addressLine2, b.city, b.state, b.postalCode, b.country, b.salesRepEmployeeNumber, b.creditLimit);
END
#################################################################################################
CREATE PROCEDURE
  DataPath_QTY.sp_my_sql_employees_qty()
BEGIN
MERGE
  terpel-pruebas-369613.DataPath_QTY.my_sql_employees_qty AS a
USING
  (
  SELECT
    employeeNumber,
    lastName,
    firstName,
    extension,
    email,
    officeCode,
    reportsTo,
    jobTitle
  FROM
    terpel-pruebas-369613.DataPath_RAW.my_sql_employees_raw) AS b
ON
  a.employeeNumber = b.employeeNumber
  WHEN NOT MATCHED THEN 
  INSERT ( employeeNumber, lastName, firstName, extension, email, officeCode, reportsTo, jobTitle) 
  VALUES ( b.employeeNumber, b.lastName, b.firstName, b.extension, b.email, b.officeCode, b.reportsTo, b.jobTitle);
END
##############################################################################################
CREATE PROCEDURE
  DataPath_QTY.sp_my_sql_offices_qty()
BEGIN
MERGE
  terpel-pruebas-369613.DataPath_QTY.my_sql_offices_qty AS a
USING
  (
  SELECT
    officeCode,
    city,
    phone,
    addressLine1,
    addressLine2,
    state,
    country,
    postalCode,
    territory
  FROM
    terpel-pruebas-369613.DataPath_RAW.my_sql_offices_raw) AS b
ON
  a.officeCode = b.officeCode
  WHEN NOT MATCHED THEN 
  INSERT ( officeCode, city, phone, addressLine1, addressLine2, state, country, postalCode, territory) 
  VALUES ( b.officeCode, b.city, b.phone, b.addressLine1, b.addressLine2, b.state, b.country, b.postalCode, b.territory);
END
#######################################################################################################
CREATE PROCEDURE
  DataPath_QTY.sp_my_sql_orders_qty()
BEGIN
MERGE
  terpel-pruebas-369613.DataPath_QTY.my_sql_orders_qty AS a
USING
  (
  SELECT
    orderNumber,
    orderDate,
    requiredDate,
    shippedDate,
    status,
    comments,
    customerNumber
  FROM
    terpel-pruebas-369613.DataPath_RAW.my_sql_orders_raw) AS b
ON
  a.orderNumber = b.orderNumber
  WHEN NOT MATCHED THEN 
  INSERT ( orderNumber, orderDate, requiredDate, shippedDate, status, comments, customerNumber) 
  VALUES ( b.orderNumber, b.orderDate, b.requiredDate, b.shippedDate, b.status, b.comments, b.customerNumber);
END
##############################################################################################
CREATE PROCEDURE
  DataPath_QTY.sp_my_sql_payments_qty()
BEGIN
MERGE
  terpel-pruebas-369613.DataPath_QTY.my_sql_payments_qty AS a
USING
  (
  SELECT
customerNumber, checkNumber, paymentDate, amount
  FROM
    terpel-pruebas-369613.DataPath_RAW.my_sql_payments_raw) AS b
ON
  a.customerNumber = b.customerNumber
  WHEN NOT MATCHED THEN 
  INSERT ( customerNumber, checkNumber, paymentDate, amount) 
  VALUES ( b.customerNumber, b.checkNumber, b.paymentDate, b.amount);
END
###################################################################################################
CREATE PROCEDURE
  DataPath_QTY.sp_my_sql_productlines_qty()
BEGIN
MERGE
  terpel-pruebas-369613.DataPath_QTY.my_sql_productlines_qty AS a
USING
  (
  SELECT
productLine, textDescription, htmlDescription, image
  FROM
    terpel-pruebas-369613.DataPath_RAW.my_sql_productlines_raw) AS b
ON
  a.productLine = b.productLine
  WHEN NOT MATCHED THEN 
  INSERT ( productLine, textDescription, htmlDescription, image) 
  VALUES ( b.productLine, b.textDescription, b.htmlDescription, b.image);
END
####################################################################################################
CREATE PROCEDURE
  DataPath_QTY.sp_my_sql_products_qty()
BEGIN
MERGE
  terpel-pruebas-369613.DataPath_QTY.my_sql_products_qty AS a
USING
  (
  SELECT
productCode, productName, productLine, productScale, productVendor, productDescription, quantityInStock, buyPrice, MSRP
  FROM
    terpel-pruebas-369613.DataPath_RAW.my_sql_products_raw) AS b
ON
  a.productCode = b.productCode
  WHEN NOT MATCHED THEN 
  INSERT ( productCode, productName, productLine, productScale, productVendor, productDescription, quantityInStock, buyPrice, MSRP) 
  VALUES ( b.productCode, b.productName, b.productLine, b.productScale, b.productVendor, b.productDescription, b.quantityInStock, b.buyPrice, b.MSRP);
END
###################################################################################################
CREATE PROCEDURE
  `terpel-pruebas-369613.DataPath_QTY.sp_orquestador` ()
BEGIN
CALL
  `terpel-pruebas-369613.DataPath_QTY.sp_bucket_orderdetails_qty`();
CALL
  `terpel-pruebas-369613.DataPath_QTY.sp_my_sql_customers_qty`();
CALL
  `terpel-pruebas-369613.DataPath_QTY.sp_my_sql_employees_qty`();
CALL
  `terpel-pruebas-369613.DataPath_QTY.sp_my_sql_offices_qty`();
CALL
  `terpel-pruebas-369613.DataPath_QTY.sp_my_sql_orders_qty`();
CALL
  `terpel-pruebas-369613.DataPath_QTY.sp_my_sql_payments_qty`();
CALL
  `terpel-pruebas-369613.DataPath_QTY.sp_my_sql_productlines_qty`();
CALL
  `terpel-pruebas-369613.DataPath_QTY.sp_my_sql_products_qty`();
END