CREATE TABLE CONFIG_DB.SMA_DETAILS
(
SMA_NAME varchar2(200) NOT NULL,
DEVICE_GROUP_NAME varchar2(100),
ADDITIONAL_CRITERIA varchar2(1000),
DESTINATION_DB varchar2(50),
DESTINATION_TABLE varchar2(100),
DESTINATION_USER_ID varchar2(20),
DESTINATION_PW varchar2(20)
);

INSERT INTO CONFIG_DB.SMA_DETAILS
VALUES (	'Nokia_7750_SEGW', 	'Nokia_7750_SEGW', 	null, 	null, 	'PMMCOUNTER_DB.SMA_NOKIA7750_SEGW', 	null, 	null);

CREATE TABLE CONFIG_DB.SMA_KPI_DETAILS
(
SMA_NAME varchar2(200) NOT NULL,
KPI_NAME varchar2(100) NOT NULL,
UNITS varchar2(50) NOT NULL,
SOURCE_BASE_TABLE varchar2(100) NOT NULL,
KPI_EXPRESSION varchar2(500) NOT NULL,
ADDITIONAL_CRITERIA varchar2(1000),
AGGR_TO_DEVICE varchar2(30),
AGGR_5M varchar2(30),
AGGR_15M varchar2(30),
AGGR_HH varchar2(30),
AGGR_DY varchar2(30),
AGGR_IW varchar2(30),
AGGR_MO varchar2(30),
AGGR_TABLE_EXT_5M varchar2(20),
AGGR_TABLE_EXT_15M varchar2(20),
AGGR_TABLE_EXT_HH varchar2(20),
AGGR_TABLE_EXT_DY varchar2(20),
AGGR_TABLE_EXT_IW varchar2(20),
AGGR_TABLE_EXT_MO varchar2(20)
);

INSERT INTO CONFIG_DB.SMA_KPI_DETAILS
VALUES ( 'Nokia_7750_SEGW',  '% CPU Utilization - MAX',  '%',  'ALU_IP.ALU_IPNE_SYSTEM_STAT',  'CPU_UTILIZATION_MAX',  null,   'MAX',  'MAX',  'MAX',  'MAX',  'MAX',  'MAX',  'MAX',  '5M',   '5M',   'HR',   'DY',   'WK',   'MO');
INSERT INTO CONFIG_DB.SMA_KPI_DETAILS
VALUES ( 'Nokia_7750_SEGW',  '% Memory Used - MAX',  '%',  'ALU_IP.ALU_IPNE_SYSTEM_STAT',  'MEMORYUSAGEPERCENTAGE',  null,   'MAX',  'MAX',  'MAX',  'MAX',  'MAX',  'MAX',  'MAX',  '5M',   '5M',   'HR',   'DY',   'WK',   'MO');
INSERT INTO CONFIG_DB.SMA_KPI_DETAILS
VALUES ( 'Nokia_7750_SEGW',  'Memory Available - MIN',   'Bytes',  'ALU_IP.ALU_IPNE_SYSTEM_STAT',  'SGI_MEMORY_AVAILABLE',   null,   'MIN',  'MIN',  'MIN',  'MIN',  'MIN',  'MIN',  'MIN',  '5M',   '5M',   'HR',   'DY',   'WK',   'MO');
INSERT INTO CONFIG_DB.SMA_KPI_DETAILS
VALUES ( 'Nokia_7750_SEGW',  'Memory Pool Allocated - MIN',  'Bytes',  'ALU_IP.ALU_IPNE_SYSTEM_STAT',  'SGI_MEMORY_POOL_ALLOCATED',  null,   'MIN',  'MIN',  'MIN',  'MIN',  'MIN',  'MIN',  'MIN',  '5M',   '5M',   'HR',   'DY',   'WK',   'MO');
INSERT INTO CONFIG_DB.SMA_KPI_DETAILS
VALUES ( 'Nokia_7750_SEGW',  'Throughput in - MAX',  'bits/sec',   'ALL_IP.STD_IPIF',  'IF_HCIN_OCTETS * 8 / SYS_UP_TIME_D',   null,   'SUM',  'MAX',  'MAX',  'MAX',  'MAX',  'MAX',  'MAX',  '5M',   '5M',   'HR',   'DY',   'WK',   'MO');
INSERT INTO CONFIG_DB.SMA_KPI_DETAILS
VALUES ( 'Nokia_7750_SEGW',  'Throughput in - AVG',  'bits/sec',   'ALL_IP.STD_IPIF',  'IF_HCIN_OCTETS * 8 / SYS_UP_TIME_D',   null,   'SUM',  'AVG',  'AVG',  'AVG',  'AVG',  'AVG',  'AVG',  '5M',   '5M',   'HR',   'DY',   'WK',   'MO');
INSERT INTO CONFIG_DB.SMA_KPI_DETAILS
VALUES ( 'Nokia_7750_SEGW',  'Throughput out - MAX',   'bits/sec',   'ALL_IP.STD_IPIF',  'IF_HCOUT_OCTETS * 8 / SYS_UP_TIME_D',  null,   'SUM',  'MAX',  'MAX',  'MAX',  'MAX',  'MAX',  'MAX',  '5M',   '5M',   'HR',   'DY',   'WK',   'MO');
INSERT INTO CONFIG_DB.SMA_KPI_DETAILS
VALUES ( 'Nokia_7750_SEGW',  'Throughput out - AVG',   'bits/sec',   'ALL_IP.STD_IPIF',  'IF_HCOUT_OCTETS * 8 / SYS_UP_TIME_D',  null,   'SUM',  'AVG',  'AVG',  'AVG',  'AVG',  'AVG',  'AVG',  '5M',   '5M',   'HR',   'DY',   'WK',   'MO');
INSERT INTO CONFIG_DB.SMA_KPI_DETAILS
VALUES ( 'Nokia_7750_SEGW',  'IPSec Cert Tunnels - MAX',   'Count',  'ALU_IP.ALU_IPNE_TUNNEL_UTIL',  'TMNX_IPSEC_GWCERT_TUNNELS_MAX',  null,   'SUM',  'MAX',  'MAX',  'MAX',  'MAX',  'MAX',  'MAX',  '5M',   '5M',   'HR',   'DY',   'WK',   'MO');
INSERT INTO CONFIG_DB.SMA_KPI_DETAILS
VALUES ( 'Nokia_7750_SEGW',  'Ipsec Isa Group Tunnels - MAX',  'Count',  'ALU_IP.ALU_IPNE_IPSEC_TUNN',   'TMNX_IPSEC_ISA_GRP_TUNNELS',   null,   'SUM',  'MAX',  'MAX',  'MAX',  'MAX',  'MAX',  'MAX',  '5M',   '5M',   'HR',   'DY',   'WK',   'MO');
INSERT INTO CONFIG_DB.SMA_KPI_DETAILS
VALUES ( 'Nokia_7750_SEGW',  'Ipsec Isa Group Max Tunnels - MAX',  'Count',  'ALU_IP.ALU_IPNE_IPSEC_TUNN',   'TMNX_IPSEC_ISA_GRP_MAX_TUNNELS',   null,   'SUM',  'MAX',  'MAX',  'MAX',  'MAX',  'MAX',  'MAX',  '5M',   '5M',   'HR',   'DY',   'WK',   'MO');

CREATE TABLE CONFIG_DB.SMA_DEVICE_LIST
(
DEVICE_GROUP_NAME varchar2(100) NOT NULL,
DEVICE varchar2(100),
DEVICE_EXPRESSION varchar2(500)
);

INSERT INTO CONFIG_DB.SMA_DEVICE_LIST
VALUES ( 'Nokia_7750_SEGW',  'casanj01-segw-01',   null);
INSERT INTO CONFIG_DB.SMA_DEVICE_LIST
VALUES ( 'Nokia_7750_SEGW',  'gaatla01-segw-01',   null);
INSERT INTO CONFIG_DB.SMA_DEVICE_LIST
VALUES ( 'Nokia_7750_SEGW',  'ilchic01-segw-01',   null);
INSERT INTO CONFIG_DB.SMA_DEVICE_LIST
VALUES ( 'Nokia_7750_SEGW',  'vaasshb01-segw-01',  null);

CREATE TABLE PMMCOUNTER_DB.SMA_NOKIA7750_SEGW
(
DATETIME date NOT NULL,
AGG_TYPE varchar2(20) NOT NULL,
DEVICE_NAME varchar2(100) NOT NULL,
KPI_NAME varchar2(200) NOT NULL,
KPI_VALUE number NOT NULL,
KPI_UNITS varchar2(100),
REC_CREATE_DATE date default systimestamp
);
