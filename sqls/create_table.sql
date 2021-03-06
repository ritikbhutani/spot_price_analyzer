CREATE TABLE "SPOT_PRICE"."PUBLIC"."PRICE" 
(
  "ROW_ID" INTEGER NOT NULL AUTOINCREMENT START 1 INCREMENT 1, 
  "ASOF_DATE" DATE NOT NULL, 
  "AVAILABILITY_ZONE" STRING, 
  "INSTANCE_TYPE" STRING, 
  "PRODUCT_DESCRIPTION" STRING, 
  "SPOT_PRICE" NUMBER (38, 18), 
  "TIME_STAMP" TIMESTAMP
);