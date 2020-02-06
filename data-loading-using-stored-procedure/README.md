# LAB - Data Loading using Stored Procedure

In this lab, we will load the eight tables based on the TPC Benchmark data model. These tables will be created and loaded using Redshift Stored Procedure.

![](../images/Model.png)

## Contents
* [Before You Begin](#before-you-begin)
* [Cloud Formation](#cloud-formation)
* [Create Tables](#create-tables)
* [Loading Data](#loading-data)
* [Table Maintenance - ANALYZE](#table-maintenance---analyze)
* [Table Maintenance - VACUUM](#table-maintenance---vacuum)
* [Troubleshooting Loads](#troubleshooting-loads)
* [Before You Leave](#before-you-leave)

## Before You Begin
This lab assumes you have launched a Redshift cluster and can gather the following information.  If you have not launched a cluster, see [LAB 1 - Creating Redshift Clusters](https://github.com/aws-samples/redshift-immersionday-labs/blob/master/lab1/README.md).
* [Your-AWS_Account_Id]
* [Your-Redshift_Hostname]
* [Your-Redshift_Port]
* [Your-Redshift_Username]
* [Your-Redshift_Password]
* [Your-Redshift-Role]

It also assumes you have access to a configured client tool. For more details on configuring SQL Workbench/J as your client tool, see [Lab 1 - Creating Redshift Clusters : Configure Client Tool(https://github.com/aws-samples/redshift-immersionday-labs/blob/master/lab1/README.md#configure-client-tool). As an alternative you can use the Redshift provided online Query Editor which does not require an installation.
```
https://console.aws.amazon.com/redshift/home?#query:
```

## Cloud Formation
To *skip this lab* and complete the loading of this sample data into an **existing cluster** using cloud formation, use the following link.

[![Launch](../images/cloudformation-launch-stack.png)](https://console.aws.amazon.com/cloudformation/home?#/stacks/new?stackName=ImmersionLab2&templateURL=https://s3-us-west-2.amazonaws.com/redshift-immersionday-labs/lab2.yaml)

To *skip this lab* and complete the loading of this sample data into a **new cluster** using cloud formation, use the following link.

[![Launch](../images/cloudformation-launch-stack.png)](https://console.aws.amazon.com/cloudformation/home?#/stacks/new?stackName=ImmersionLab2&templateURL=https://s3-us-west-2.amazonaws.com/redshift-immersionday-labs/lab2%2Bcluster.yaml)

Note: These cloud formation templates will create a Lambda function which will trigger an asynchronous Glue Python Shell script.  The Glue script assumes that the Redshift Cluster is publically accessible with an ingress rule of 0.0.0.0/0.  To monitor the load process and diagnose any load errors, see the following Cloudwatch Logs stream:
```
https://console.aws.amazon.com/cloudwatch/home?#logStream:group=/aws-glue/python-jobs/output
```

## Create Tables
Copy the following create table statements to create tables in the database.  
```
DROP TABLE IF EXISTS partsupp_sp;
DROP TABLE IF EXISTS lineitem_sp;
DROP TABLE IF EXISTS supplier_sp;
DROP TABLE IF EXISTS part_sp;
DROP TABLE IF EXISTS orders_sp;
DROP TABLE IF EXISTS customer_sp;
DROP TABLE IF EXISTS nation_sp;
DROP TABLE IF EXISTS region_sp;

CREATE TABLE region_sp (
  R_REGIONKEY bigint NOT NULL PRIMARY KEY,
  R_NAME varchar(25),
  R_COMMENT varchar(152))
diststyle all;

CREATE TABLE nation_sp (
  N_NATIONKEY bigint NOT NULL PRIMARY KEY,
  N_NAME varchar(25),
  N_REGIONKEY bigint REFERENCES region_sp(R_REGIONKEY),
  N_COMMENT varchar(152))
diststyle all;

create table customer_sp (
  C_CUSTKEY bigint NOT NULL PRIMARY KEY,
  C_NAME varchar(25),
  C_ADDRESS varchar(40),
  C_NATIONKEY bigint REFERENCES nation_sp(N_NATIONKEY),
  C_PHONE varchar(15),
  C_ACCTBAL decimal(18,4),
  C_MKTSEGMENT varchar(10),
  C_COMMENT varchar(117))
diststyle all;

create table orders_sp (
  O_ORDERKEY bigint NOT NULL PRIMARY KEY,
  O_CUSTKEY bigint REFERENCES customer_sp(C_CUSTKEY),
  O_ORDERSTATUS varchar(1),
  O_TOTALPRICE decimal(18,4),
  O_ORDERDATE Date,
  O_ORDERPRIORITY varchar(15),
  O_CLERK varchar(15),
  O_SHIPPRIORITY Integer,
  O_COMMENT varchar(79))
distkey (O_ORDERKEY)
sortkey (O_ORDERDATE);

create table part_sp (
  P_PARTKEY bigint NOT NULL PRIMARY KEY,
  P_NAME varchar(55),
  P_MFGR  varchar(25),
  P_BRAND varchar(10),
  P_TYPE varchar(25),
  P_SIZE integer,
  P_CONTAINER varchar(10),
  P_RETAILPRICE decimal(18,4),
  P_COMMENT varchar(23))
diststyle all;

create table supplier_sp (
  S_SUPPKEY bigint NOT NULL PRIMARY KEY,
  S_NAME varchar(25),
  S_ADDRESS varchar(40),
  S_NATIONKEY bigint REFERENCES nation_sp(n_nationkey),
  S_PHONE varchar(15),
  S_ACCTBAL decimal(18,4),
  S_COMMENT varchar(101))
diststyle all;                                                              

create table lineitem_sp (
  L_ORDERKEY bigint NOT NULL REFERENCES orders_sp(O_ORDERKEY),
  L_PARTKEY bigint REFERENCES part_sp(P_PARTKEY),
  L_SUPPKEY bigint REFERENCES supplier_sp(S_SUPPKEY),
  L_LINENUMBER integer NOT NULL,
  L_QUANTITY decimal(18,4),
  L_EXTENDEDPRICE decimal(18,4),
  L_DISCOUNT decimal(18,4),
  L_TAX decimal(18,4),
  L_RETURNFLAG varchar(1),
  L_LINESTATUS varchar(1),
  L_SHIPDATE date,
  L_COMMITDATE date,
  L_RECEIPTDATE date,
  L_SHIPINSTRUCT varchar(25),
  L_SHIPMODE varchar(10),
  L_COMMENT varchar(44),
PRIMARY KEY (L_ORDERKEY, L_LINENUMBER))
distkey (L_ORDERKEY)
sortkey (L_RECEIPTDATE);

create table partsupp_sp (
  PS_PARTKEY bigint NOT NULL REFERENCES part_sp(P_PARTKEY),
  PS_SUPPKEY bigint NOT NULL REFERENCES supplier_sp(S_SUPPKEY),
  PS_AVAILQTY integer,
  PS_SUPPLYCOST decimal(18,4),
  PS_COMMENT varchar(199),
PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY))
diststyle even;
```

## Create Stored Procedure to load the data
We will use a stored procedure to load the data in the tables created above using COPY command. The stored procedure will take the input of the IAM Role which you created earlier and load the data to the tables created above. We will also have the input variable as the additional parameteres for the COPY command.

```
CREATE OR REPLACE PROCEDURE load_demodata(f1 varchar, f2 varchar, OUT varchar(256))
AS $$
DECLARE
  out_var alias for $3;

BEGIN
  IF f1 is null THEN
    RAISE EXCEPTION 'input cannot be null';
  END IF;
  
  

out_var :=  'COPY region_sp FROM ''s3://redshift-immersionday-labs/data/region/region.tbl.lzo''
iam_role '''||f1||'''
region ''us-west-2'' lzop delimiter ''|'' '||f2;

execute out_var;

out_var :=  'COPY nation_sp FROM ''s3://redshift-immersionday-labs/data/nation/nation.tbl.''
iam_role '''||f1||'''
region ''us-west-2'' lzop delimiter ''|'' '||f2;

execute out_var;

out_var :=  'COPY customer_sp FROM ''s3://redshift-immersionday-labs/data/customer/customer.tbl.''
iam_role '''||f1||'''
region ''us-west-2'' lzop delimiter ''|'' '||f2;

execute out_var;

out_var :=  'COPY orders_sp FROM ''s3://redshift-immersionday-labs/data/orders/orders.tbl.''
iam_role '''||f1||'''
region ''us-west-2'' lzop delimiter ''|'' '||f2;

execute out_var;

out_var :=  'COPY part_sp FROM ''s3://redshift-immersionday-labs/data/part/part.tbl.''
iam_role '''||f1||'''
region ''us-west-2'' lzop delimiter ''|'' '||f2;

execute out_var;

out_var :=  'COPY supplier_sp FROM ''s3://redshift-immersionday-labs/data/supplier/supplier.json'' manifest 
iam_role '''||f1||'''
region ''us-west-2'' lzop delimiter ''|'' '||f2;

execute out_var;

out_var :=  'COPY lineitem_sp FROM ''s3://redshift-immersionday-labs/data/lineitem/lineitem.tbl.''
iam_role '''||f1||'''
region ''us-west-2'' lzop delimiter ''|'' '||f2;

execute out_var;


out_var :=  'COPY partsupp_sp FROM ''s3://redshift-immersionday-labs/data/partsupp/partsupp.tbl.''
iam_role '''||f1||'''
region ''us-west-2'' lzop delimiter ''|'' '||f2;

execute out_var;

out_var := 'done';

END;
$$ LANGUAGE plpgsql;
```

## Run the Redshift stored procedure
Now run the stored procedure we created above. When you run the SP make sure you pass the IAM Role which you created in Lab 1 and attached to Redshift cluster.

```
call load_demodata('arn:aws:iam::1234567890:role/RedshiftImmersionRole','');
```

So in case you want to run the COPY command with "COMPUPDATE PRESET ON" with the stored procedure then you can run something like below:

```
call load_demodata('arn:aws:iam::1234567890:role/RedshiftImmersionRole','COMPUPDATE PRESET ON');
```

## Verify Data
Run the following queries to verify that the data loading worked

```
SELECT COUNT(*) FROM orders_sp;

SELECT "column", type, encoding 
FROM pg_table_def WHERE tablename = 'orders_sp';

analyze compression orders_sp;

select tab.table_schema,
       tab.table_name,
       tinf.tbl_rows as rows, 
       tinf.size as size_mb,
       tinf.diststyle,
       tinf.sortkey1
from svv_tables tab
join svv_table_info tinf
          on tab.table_schema = tinf.schema
          and tab.table_name = tinf.table
where tab.table_type = 'BASE TABLE'
      and tab.table_name in ('lineitem_sp','orders_sp','nation_sp','region_sp','partsupp_sp','part_sp','customer_sp','supplier_sp')
      and tinf.tbl_rows >= 1
order by tinf.tbl_rows desc;
```
