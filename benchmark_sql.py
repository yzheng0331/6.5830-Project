from dask_sql import Context
import dask.dataframe as dd
import time
from itertools import permutations

import resource
import sys

def memory_limit():
    """Limit max memory usage to half."""
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    # Convert KiB to bytes, and divide in two to half
    resource.setrlimit(resource.RLIMIT_AS, (12*1024**3, hard))

def get_memory():
    with open('/proc/meminfo', 'r') as mem:
        free_memory = 0
        for i in mem:
            sline = i.split()
            if str(sline[0]) in ('MemFree:', 'Buffers:', 'Cached:'):
                free_memory += int(sline[1])
    return free_memory  # KiB

# memory_limit()

# dask.config.set()
start = time.time()

customer = dd.read_csv("dbgen/customer.csv", sep="|", header=None, names=["CUSTKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL", "MKTSEGMET", "COMMENT"])
orders = dd.read_csv("dbgen/orders.csv", sep="|", header=None, names=["ORDERKEY", "CUSTKEY", "ORDERSTATUS", "TOTALPRICE", "ORDERDATE", "ORDERPRIORITY", "CLERK", "SHIPPRIORITY", "COMMENT"])
lineitem = dd.read_csv("dbgen/lineitem.csv", sep="|", header=None, names=["ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER", "QUANTITY", "EXTENDEDPRICE", "DISCOUNT", "TAX", "RETURNFLAG", "LINESTATUS", "SHIPDATE", "COMMITDATE", "RECEIPTDATE", "SHIPINSTRUCT", "SHIPMODE", "COMMENT"])
supplier = dd.read_csv("dbgen/supplier.csv", sep="|", header=None, names=["SUPPKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL", "COMMENT"])
# nation = dd.read_csv("dbgen/nation.csv", sep="|", header=None, names=["NATIONKEY", "NAME", "REGIONKEY", "COMMENT"])
## region = dd.read_csv("dbgen/nation.csv", sep="|", header=None, names=["NAME", "REGIONKEY", "COMMENT"])

print("load data time: ", time.time()-start)

c = Context()
# c = Context(config={"sql.defaults.cache_size": "1GB"})

c.create_table("customer", customer)
c.create_table("orders", orders)
c.create_table("lineitem", lineitem)
c.create_table("supplier", supplier)
# c.create_table("nation", nation)
# c.create_table("region", region)

joins = ['"lineitem"."ORDERKEY" = "orders"."ORDERKEY"', 
         '"customer"."CUSTKEY" = "orders"."CUSTKEY"', 
         '"lineitem"."SUPPKEY" = "supplier"."SUPPKEY"',
         '"customer"."NATIONKEY" = "supplier"."NATIONKEY"']

query = """
SELECT *
FROM
    customer,
    orders,
    lineitem,
    supplier
WHERE 
"""

with open('permutations.txt', 'w') as file:        
    for p in permutations(joins):
        j1, j2, j3, j4 = p
        query_f = query + j1 + "AND" + j2 + "AND" + j3 + "AND" + j4 +";"
        before_query = time.time()
        result = c.sql(query_f).compute()
        t = time.time()-before_query
        le = len(result)
        print("cyclic join time: ", t, le)
        file.write(' '.join(map(str, p)) + ' :' + str(t) + ' ' + str(le) + '\n')

        # print(len(result))
        # break

# query = """
# select
# 	"nation"."NAME"
# from
# 	customer,
# 	orders,
# 	lineitem,
# 	supplier,
# 	nation
# where
#     "customer"."CUSTKEY" = "orders"."CUSTKEY"
#     and "lineitem"."ORDERKEY" = "orders"."ORDERKEY"
#     and "lineitem"."SUPPKEY" = "supplier"."SUPPKEY"
#     and "customer"."NATIONKEY" = "supplier"."NATIONKEY"
#     and "supplier"."NATIONKEY" = "nation"."NATIONKEY";
# """

# before_query = time.time()
# result = c.sql(query).compute()
# t = time.time()-before_query
# print("cyclic join time: ", t)

# print(len(result))


# query = """
# SELECT *
# FROM
#     customer,
#     orders,
#     lineitem,
#     supplier
# WHERE
#     "lineitem"."ORDERKEY" = "orders"."ORDERKEY"
#     AND "customer"."CUSTKEY" = "orders"."CUSTKEY"
#     AND "lineitem"."SUPPKEY" = "supplier"."SUPPKEY"
#     AND "customer"."NATIONKEY" = "supplier"."NATIONKEY";
# """


# before_query = time.time()
# result = c.sql(query).compute()
# t = time.time()-before_query
# print("cyclic join time: ", t)


# """
# SELECT *
# FROM
#     customer,
#     orders,
#     lineitem,
#     supplier
# WHERE
#     "lineitem"."ORDERKEY" = "orders"."ORDERKEY"
#     AND "customer"."CUSTKEY" = "orders"."CUSTKEY"
#     AND "lineitem"."SUPPKEY" = "supplier"."SUPPKEY"
#     AND "customer"."NATIONKEY" = "supplier"."NATIONKEY";
# """



# query = """
# select
# 	"nation"."NAME",
# 	sum("lineitem"."EXTENDEDPRICE" * (1 - "lineitem"."DISCOUNT")) as revenue
# from
# 	customer,
# 	orders,
# 	lineitem,
# 	supplier,
# 	nation,
## 	region
# where
#     "customer"."CUSTKEY" = "orders"."CUSTKEY"
#     and "lineitem"."ORDERKEY" = "orders"."ORDERKEY"
#     and "lineitem"."SUPPKEY" = "supplier"."SUPPKEY"
#     and "customer"."NATIONKEY" = "supplier"."NATIONKEY"
#     and "supplier"."NATIONKEY" = "nation"."NATIONKEY"
#     and "nation"."REGIONKEY" = "region"."REGIONKEY"
# 	and "region"."NAME" = 'ASIA'
# 	and "orders"."ORDERDATE" >= date '1994-01-01'
# 	and "orders"."ORDERDATE" < date '1994-01-01' + interval '1' year
# group by
# 	"nation"."NAME"
# order by
# 	revenue desc;
# """