CREATE TABLE "PMMCONF_DB"."SMA_DETAILS" 
   (  "SMA_NAME" VARCHAR2(200) NOT NULL ENABLE, 
  "DEVICE_GROUP_NAME" VARCHAR2(100), 
  "ADDITIONAL_CRITERIA" VARCHAR2(1000), 
  "DESTINATION_DB" VARCHAR2(50), 
  "DESTINATION_TABLE" VARCHAR2(100), 
  "DESTINATION_USER_ID" VARCHAR2(20), 
  "DESTINATION_PW" VARCHAR2(20), 
  "ACTIVE" NUMBER DEFAULT 1, 
  "DESTINATION_DB_HOST" VARCHAR2(255)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "PMMCONF_DB_DATA" ;
  CREATE UNIQUE INDEX "PMMCONF_DB"."SMA_DETAILS_IDX1" ON "PMMCONF_DB"."SMA_DETAILS" ("SMA_NAME") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "PMMCONF_DB_DATA" ;



  CREATE TABLE "PMMCONF_DB"."SMA_DEVICE_LIST" 
   (  "DEVICE_GROUP_NAME" VARCHAR2(100) NOT NULL ENABLE, 
  "DEVICE" VARCHAR2(100), 
  "DEVICE_EXPRESSION" VARCHAR2(500)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "PMMCONF_DB_DATA" ;


    CREATE UNIQUE INDEX "PMMCONF_DB"."SMA_DEVICE_LIST_IDX1" ON "PMMCONF_DB"."SMA_DEVICE_LIST" ("DEVICE_GROUP_NAME");


  CREATE TABLE "PMMCONF_DB"."SMA_KPI_DATE_CONTROL" 
   (  "SMA_NAME" VARCHAR2(200) NOT NULL ENABLE, 
  "SOURCE_BASE_TABLE" VARCHAR2(100) NOT NULL ENABLE, 
  "RESOLUTION" VARCHAR2(50) NOT NULL ENABLE, 
  "LAST_HANDLED_DATESTAMP" TIMESTAMP (6)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "PMMCONF_DB_DATA" ;



  CREATE TABLE "PMMCONF_DB"."SMA_KPI_DETAILS" 
   (  "SMA_NAME" VARCHAR2(200) NOT NULL ENABLE, 
  "KPI_NAME" VARCHAR2(100) NOT NULL ENABLE, 
  "UNITS" VARCHAR2(50) NOT NULL ENABLE, 
  "SOURCE_BASE_TABLE" VARCHAR2(100) NOT NULL ENABLE, 
  "KPI_EXPRESSION" VARCHAR2(4000) NOT NULL ENABLE, 
  "ADDITIONAL_CRITERIA" VARCHAR2(1000), 
  "AGGR_TO_DEVICE" VARCHAR2(30), 
  "AGGR_5M" VARCHAR2(30), 
  "AGGR_15M" VARCHAR2(30), 
  "AGGR_HH" VARCHAR2(30), 
  "AGGR_DY" VARCHAR2(30), 
  "AGGR_IW" VARCHAR2(30), 
  "AGGR_MO" VARCHAR2(30), 
  "AGGR_TABLE_EXT_5M" VARCHAR2(20), 
  "AGGR_TABLE_EXT_15M" VARCHAR2(20), 
  "AGGR_TABLE_EXT_HH" VARCHAR2(20), 
  "AGGR_TABLE_EXT_DY" VARCHAR2(20), 
  "AGGR_TABLE_EXT_IW" VARCHAR2(20), 
  "AGGR_TABLE_EXT_MO" VARCHAR2(20), 
  "ACTIVE" NUMBER DEFAULT 1, 
  "DEVICE_FIELD_NAME" VARCHAR2(255), 
  "RUN_NOW" NUMBER DEFAULT 0, 
  "DEVICE_TARGET_FIELD" VARCHAR2(255) DEFAULT 'DEVICE_NAME'
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "PMMCONF_DB_DATA" ;

    CREATE UNIQUE INDEX "PMMCONF_DB"."SMA_KPI_DETAILS_IDX1" ON "PMMCONF_DB"."SMA_KPI_DETAILS" ("SMA_NAME","KPI_NAME");
