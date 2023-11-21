import dask.dataframe as dd
import dask
from memory_profiler import profile
import cProfile
import pstats
import io
import time
import pandas as pd

import resource
import sys

def memory_limit_half(limit = 0):
    """Limit max memory usage to half."""
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    # Convert KiB to bytes, and divide in two to half
    if limit == 0:
        resource.setrlimit(resource.RLIMIT_AS, (int(get_memory() * 1024 / 2), hard))
    else:
        resource.setrlimit(resource.RLIMIT_AS, (limit * 1024 * 1024 * 1024, hard))

def get_memory():
    with open('/proc/meminfo', 'r') as mem:
        free_memory = 0
        for i in mem:
            sline = i.split()
            if str(sline[0]) in ('MemFree:', 'Buffers:', 'Cached:'):
                free_memory += int(sline[1])
    print("currently we have free memory of", free_memory, "kb")
    return free_memory  # KiB

# @profile
@dask.delayed
def get_region():
    region = dd.read_csv('dbgen/region.csv',sep='|', header=None,names=["REGIONKEY","NAME","COMMENT"])#.compute()
    region = region.set_index('REGIONKEY')
    print(region.describe())
    return region
# @profile
@dask.delayed
def get_nation():
    nation = dd.read_csv('dbgen/nation.csv',sep='|', header=None,names=["NATIONKEY","NAME","REGIONKEY","COMMENT"])#.compute()
    nation = nation.set_index('NATIONKEY')
    print(nation.describe())
    return nation
# @profile
@dask.delayed
def get_supplier():
    supplier = dd.read_csv('dbgen/supplier.csv',sep='|', header=None,names=["SUPPKEY","NAME","ADDRESS","NATIONKEY","PHONE","ACCTBAL","COMMENT"])#.compute()
    supplier = supplier.set_index('SUPPKEY')
    print(supplier.describe())
    return supplier
# @profile
@dask.delayed
def get_orders(nrows = 0):
    #1.7G
    if nrows > 0:
        orders = dd.read_csv('dbgen/orders.csv',sep='|', header=None,
                             names=["ORDERKEY","CUSTKEY","ORDERSTATUS","TOTALPRICE","ORDERDATE","ORDERPRIORITY","CLERK","SHIPPRIORITY","COMMENT"],
                             ).head(n=nrows,npartitions=-1,compute= False).repartition(npartitions = 10)#.set_index("ORDERKEY")#.compute()
    else:
        orders = dd.read_csv('dbgen/orders.csv',sep='|', header=None,
                             names=["ORDERKEY","CUSTKEY","ORDERSTATUS","TOTALPRICE","ORDERDATE","ORDERPRIORITY","CLERK","SHIPPRIORITY","COMMENT"])#.set_index("ORDERKEY")
    # orders = orders.set_index('ORDERKEY')
    # print(orders.describe())
    print("order npartition:",orders.npartitions)
    #print("loading orders")
    return orders
# @profile
@dask.delayed
def get_customer():
    customer = dd.read_csv('dbgen/customer.csv',sep='|', header=None,names=["CUSTKEY","NAME","ADDRESS","NATIONKEY","PHONE","ACCTBAL","MKTSEGMET","COMMENT"])#.compute()
    customer = customer.set_index('CUSTKEY')
    print(customer.describe())
    return customer
# @profile
@dask.delayed
def get_partsupp():
    #1.2G
    partsupp = dd.read_csv('dbgen/partsupp.csv',sep='|', header=None,names=["PARTKEY","SUPPKEY","AVAILQTY","SUPPLYCOST","COMMENT"])#.compute()
    partsupp["PARTSUPPKEY"] = partsupp['PARTKEY'].astype(str) + partsupp['SUPPKEY'].astype(str) 
    partsupp = partsupp.set_index("PARTSUPPKEY")
    print(partsupp.describe())
    return partsupp
# @profile
@dask.delayed
def get_lineitem(nrows = 0):
    #7.2G
    if nrows > 0:
        lineitem = dd.read_csv('dbgen/lineitem.csv',sep='|', header=None,
                           names=["ORDERKEY","PARTKEY","SUPPKEY","LINENUMBER","QUANTITY","EXTENDEDPRICE","DISCOUNT","TAX","RETURNFLAG","LINESTATUS","SHIPDATE","COMMITDATE","RECEIPTDATE","SHIPINSTRUCT","SHIPMODE","COMMENT"],
                           ).head(n=nrows,npartitions=-1,compute= False).repartition(npartitions = int(nrows/100000))#.set_index("ORDERKEY")#.compute()
    else:
        lineitem = dd.read_csv('dbgen/lineitem.csv',sep='|', header=None,
                           names=["ORDERKEY","PARTKEY","SUPPKEY","LINENUMBER","QUANTITY","EXTENDEDPRICE","DISCOUNT","TAX","RETURNFLAG","LINESTATUS","SHIPDATE","COMMITDATE","RECEIPTDATE","SHIPINSTRUCT","SHIPMODE","COMMENT"])#s.set_index("ORDERKEY")
    # lineitem["LINEKEY"] = lineitem['ORDERKEY'].astype(str) + lineitem['LINENUMBER'].astype(str) 
    # lineitem = lineitem.set_index("LINEKEY")
    # print(lineitem.describe())
    print("lineitem npartition:",lineitem.npartitions)
    #print("loading lineitem")
    return lineitem
# @profile
@dask.delayed
def get_part():
    part = dd.read_csv('dbgen/part.csv',sep='|', header=None,names=["PARTKEY","NAME","MFGR","BRAND","TYPE","SIZE","CONTAINER","RETAILPRICE","COMMENT"]).set_index("PARTKEY")#.compute()
    part = part.set_index("PARTKEY")
    print(part.describe())
    return part

@profile
def main_dask(ln,on):
    # overwrite default with single-threaded scheduler (so that we can better profile)
    dask.config.set(scheduler='synchronous')
    df = None
    c = 0
    # print("start iteration")
    # for ln in range(10,600,20):
    #     for on in range(10,150,10):
    data = {}
    data["ln"] = ln
    data["on"] = on
        #only hash join with no index is dependent on npartitions
    start = time.time()
    lineitem = get_lineitem(ln * 100000) #npartition = 120
    order = get_orders(on * 100000)#npartition = 27
    re = lineitem.merge(order,how = "left",on = "ORDERKEY")
    data["setupt"] = time.time()-start
    start = time.time()
    re = re.compute()
    data["computet"] = time.time()-start
    df_dictionary = pd.DataFrame(data,index = [0])
        #     df = pd.concat([df, df_dictionary], ignore_index=True)
    df_dictionary.to_csv("dask_no_index.csv",mode='a', index=False, header=False)
    del lineitem
    del order
    del re
    # re.visualize(filename='merge.svg')
    # re.visualize(filename="transpose_opt.svg", optimize_graph=True)

# @profile
def main_npartition(npl,npo):
    dask.config.set(scheduler='synchronous')
    # df = None
    # c = 0
    if True:
        data = {}
        data["npartition_line"] = npl 
        data["npartition_order"] = npo 
        #only hash join with no index is dependent on npartitions
        start = time.time()
        lineitem = get_lineitem(0) #npartition = 120
        order = get_orders(0)#npartition = 27
        lineitem.repartition(npartitions = 50)
        order.repartition(npartitions = 50)
        re = lineitem.merge(order,how = "left",on = "ORDERKEY",npartitions =50)
        data["setupt"] = time.time()-start
        start = time.time()
        re = re.compute()
        data["computet"] = time.time()-start
        #print(len(re.partitions[0])*np)
        # if df is None:
        #     df = pd.DataFrame(data, index=[0])
        # else:
        df_dictionary = pd.DataFrame(data,index = [0])
        #     df = pd.concat([df, df_dictionary], ignore_index=True)
        df_dictionary.to_csv("dask_npartition_result6.csv",mode='a', index=False, header=False)
        del lineitem
        del order
        del re

@profile
def main_pandas():
    df = None
    c = 0
    for ln in range(10,400,20):
        start = time.time()
        lineitem = pd.read_csv('dbgen/lineitem.csv',sep='|', header=None,
                           names=["ORDERKEY","PARTKEY","SUPPKEY","LINENUMBER","QUANTITY","EXTENDEDPRICE","DISCOUNT","TAX","RETURNFLAG","LINESTATUS","SHIPDATE","COMMITDATE","RECEIPTDATE","SHIPINSTRUCT","SHIPMODE","COMMENT"],
                           nrows = ln * 100000)
        loadl = time.time()-start
        start = time.time()
        lineitem = lineitem.set_index("ORDERKEY")
        indexl = time.time()-start
        #print(lineitem.describe())
        for on in range(10,150,10):
            data = {}
            data["ln"] = ln
            data["on"] = on
            data["loadl"] = loadl
            data["indexl"] = indexl
            start = time.time()
            orders = pd.read_csv('dbgen/orders.csv',sep='|', header=None,
                                names=["ORDERKEY","CUSTKEY","ORDERSTATUS","TOTALPRICE","ORDERDATE","ORDERPRIORITY","CLERK","SHIPPRIORITY","COMMENT"],
                                nrows = on * 100000)#.compute()
            data["loado"] = time.time()-start
            start = time.time()
            orders = orders.set_index('ORDERKEY')
            data["indexo"] = time.time()-start
            #print(orders.describe())
            start = time.time()
            re = lineitem.merge(orders,how = "left",right_index =  True,left_index = True)
            data["mergel"] = len(re)
            data["merget"] = time.time()-start 
            data["sizeo"] = orders.memory_usage(deep=True).sum()
            data["sizel"] = lineitem.memory_usage(deep=True).sum()
            if df is None:
                df = pd.DataFrame(data, index=[0])
                df.to_csv("pandas_result.csv")
            else:
                df_dictionary = pd.DataFrame(data,index = [c])
                df = pd.concat([df, df_dictionary], ignore_index=True)
                df.to_csv("pandas_result.csv",mode='a', index=False, header=False)
                # df = df.append(data, ignore_index=True)
            c += 1
            del orders
            del re  
        print(ln)
        del lineitem
    

pr = cProfile.Profile()
pr.enable()

memory_limit_half(limit = 1)
# try:
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--npl', type =int)
parser.add_argument('--npo', type =int)
parser.add_argument('--ln', type =int)
parser.add_argument('--on', type =int)
args = parser.parse_args()

main_npartition(args.npl,args.npo)
# except MemoryError:
#     sys.stderr.write('\n\nERROR: Memory Exception\n')
#     sys.exit(1)


pr.disable()
s = io.StringIO()
ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
ps.print_stats()

with open('test.txt', 'w+') as f:
    f.write(s.getvalue())
    