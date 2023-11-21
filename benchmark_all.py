import dask.dataframe as dd
import dask
from memory_profiler import profile
import cProfile
import pstats
import io
import time
import resource
from dask.distributed import LocalCluster, Client

# @profile
@dask.delayed
def get_region():
    region = dd.read_csv('dbgen/region.csv',sep='|', header=None,names=["REGIONKEY","NAME","COMMENT"])#.compute()
    ## region = region.set_index('REGIONKEY') 
    # print(region.describe())
    return region
# @profile
@dask.delayed
def get_nation():
    nation = dd.read_csv('dbgen/nation.csv',sep='|', header=None,names=["NATIONKEY","NAME","REGIONKEY","COMMENT"])#.compute()
    # nation = nation.set_index('NATIONKEY')
    # print(nation.describe())
    return nation
# @profile
@dask.delayed
def get_supplier():
    supplier = dd.read_csv('dbgen/supplier.csv',sep='|', header=None,names=["SUPPKEY","NAME","ADDRESS","NATIONKEY","PHONE","ACCTBAL","COMMENT"])#.compute()
    # supplier = supplier.set_index('SUPPKEY')
    # print(supplier.describe())
    return supplier
# @profile
@dask.delayed
def get_orders():
    #1.7G
    orders = dd.read_csv('dbgen/orders.csv',sep='|', header=None,names=["ORDERKEY","CUSTKEY","ORDERSTATUS","TOTALPRICE","ORDERDATE","ORDERPRIORITY","CLERK","SHIPPRIORITY","COMMENT"])#.compute()
    # orders = orders.set_index('ORDERKEY')
    # print(orders.describe())
    return orders
# @profile
@dask.delayed
def get_customer():
    customer = dd.read_csv('dbgen/customer.csv',sep='|', header=None,names=["CUSTKEY","NAME","ADDRESS","NATIONKEY","PHONE","ACCTBAL","MKTSEGMET","COMMENT"])#.compute()
    # customer = customer.set_index('CUSTKEY')
    # print(customer.describe())
    return customer
# @profile
@dask.delayed
def get_partsupp():
    #1.2G
    partsupp = dd.read_csv('dbgen/partsupp.csv',sep='|', header=None,names=["PARTKEY","SUPPKEY","AVAILQTY","SUPPLYCOST","COMMENT"])#.compute()
    partsupp["PARTSUPPKEY"] = partsupp['PARTKEY'].astype(str) + partsupp['SUPPKEY'].astype(str) 
    # partsupp = partsupp.set_index("PARTSUPPKEY")
    # print(partsupp.describe())
    return partsupp
# @profile
@dask.delayed
def get_lineitem():
    #7.2G
    lineitem = dd.read_csv('dbgen/lineitem.csv',sep='|', header=None, blocksize="64MB",
                           names=["ORDERKEY","PARTKEY","SUPPKEY","LINENUMBER","QUANTITY","EXTENDEDPRICE","DISCOUNT","TAX","RETURNFLAG","LINESTATUS","SHIPDATE","COMMITDATE","RECEIPTDATE","SHIPINSTRUCT","SHIPMODE","COMMENT"])#.compute()
    #lineitem["LINEKEY"] = lineitem['ORDERKEY'].astype(str) + lineitem['LINENUMBER'].astype(str) 
    # lineitem = lineitem.set_index("ORDERKEY")
    # print(lineitem.describe())
    return lineitem
# @profile
@dask.delayed
def get_part():
    part = dd.read_csv('dbgen/part.csv',sep='|', header=None,names=["PARTKEY","NAME","MFGR","BRAND","TYPE","SIZE","CONTAINER","RETAILPRICE","COMMENT"]).set_index("PARTKEY")#.compute()
    # part = part.set_index("PARTKEY")
    # print(part.describe())
    return part

def co_sl():
    start = time.time()
    lineitem = get_lineitem()
    supplier = get_supplier()
    sl = supplier.merge(lineitem, how = "right", on = 'SUPPKEY')
    sl = sl.compute()

    order = get_orders()
    customer = get_customer()
    co = customer.merge(order, how = 'right', on = 'CUSTKEY')
    co = co.compute()

    slco = sl.merge(co, how = "left", on = ['ORDERKEY', 'NATIONKEY'])
    slco = slco.compute()
    print("total time: ", time.time() - start)
    print("length: ", len(slco))

def lo_sc():
    start = time.time()
    lineitem = get_lineitem()
    order = get_orders()    
    lo = order.merge(lineitem, how = "right", on = 'ORDERKEY')
    lo = lo.compute()
    
    supplier = get_supplier()
    customer = get_customer()
    sc = customer.merge(supplier, how = 'right', on = 'NATIONKEY')
    sc = sc.compute()

    slco = sc.merge(lo, how = "right", on = ['SUPPKEY', 'CUSTKEY'])
    slco = slco.compute()
    print("total time: ", time.time() - start)
    print("length: ", len(slco))

def sl_o_c():
    start = time.time()
    lineitem = get_lineitem()
    supplier = get_supplier()
    sl = supplier.merge(lineitem, how = "right", on = 'SUPPKEY')
    sl = sl.compute()

    order = get_orders()             
    slo = sl.merge(order.compute(), how = 'left', on = 'ORDERKEY')
    # slo = slo.compute()
    
    customer = get_customer()
    slco = slo.merge(customer.compute(), how = "left", on = ['CUSTKEY', 'NATIONKEY'])
    slco = slco.compute()
    print("total time: ", time.time() - start)
    print("length: ", len(slco))

def sl_c_o():
    start = time.time()
    lineitem = get_lineitem()
    supplier = get_supplier()
    sl = supplier.merge(lineitem, how = "right", on = 'SUPPKEY')
    sl = sl.compute()

    customer = get_customer()
    slc = sl.merge(customer.compute(), how = 'left', on = 'NATIONKEY')
    
    order = get_orders()   
    slco = slc.merge(order.compute(), how = "left", on = ['CUSTKEY', 'ORDERKEY'])
    slco = slco.compute()
    print("total time: ", time.time() - start)
    print("length: ", len(slco))

def lo_c_s():
    start = time.time()
    lineitem = get_lineitem()
    order = get_orders()    
    lo = order.merge(lineitem, how = "right", on = 'ORDERKEY')
    lo = lo.compute()
    
    customer = get_customer()
    loc = lo.merge(customer.compute(), how = 'left', on = 'CUSTKEY')

    supplier = get_supplier()
    slco = loc.merge(supplier.compute(), how = "left", on = ['SUPPKEY', 'NATIONKEY'])
    slco = slco.compute()
    print("total time: ", time.time() - start)
    print("length: ", len(slco))

def cs_o_l():
    start = time.time()
    supplier = get_supplier()
    customer = get_customer()
    sc = customer.merge(supplier, how = 'right', on = 'NATIONKEY')
    sc = sc.compute()
    
    order = get_orders()
    sco = sc.merge(order.compute(), how = 'left', on = 'CUSTKEY')

    lineitem = get_lineitem()
    slco = sco.merge(lineitem.compute(), how = "left", on = ['SUPPKEY', 'ORDERKEY'])
    slco = slco.compute()
    print("total time: ", time.time() - start)
    print("length: ", len(slco))

@profile
def main():
    dask.config.set(scheduler='synchronous')  # overwrite default with single-threaded scheduler (so that we can better profile)
    # co_sl()
    # lo_sc()
    cs_o_l()

    # print("finish getting customer:",time.time()-start)
    # order = get_orders()
    # print("finish getting order:",time.time()-start)
    # re = order.merge(lineitem,how = "right",right_index =  True,left_index = True)
    # print("finish setting merge:",time.time()-start)
    # after_merge = re.compute()
    # print("merged dataset length",len(after_merge))
    # print("finish computing:",time.time()-start)
    # print(time.time()-start)

    # re.visualize(filename='merge.svg')
    # re.visualize(filename="transpose_opt.svg", optimize_graph=True)


def memory_limit():
    """Limit max memory usage to half."""
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    # Convert KiB to bytes, and divide in two to half
    resource.setrlimit(resource.RLIMIT_AS, (get_memory()*1024, hard))

def get_memory():
    with open('/proc/meminfo', 'r') as mem:
        free_memory = 0
        for i in mem:
            sline = i.split()
            if str(sline[0]) in ('MemFree:', 'Buffers:', 'Cached:'):
                free_memory += int(sline[1])
    print(free_memory)
    return 12*1024*1024  # KiB

# memory_limit()

# cluster = LocalCluster(n_workers=1,
#                        threads_per_worker=1,
#                        memory_target_fraction=0.95,
#                        memory_limit='12GB')
# client = Client(cluster)

pr = cProfile.Profile()
pr.enable()

my_result = main()

pr.disable()
s = io.StringIO()
ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
ps.print_stats()

with open('test.txt', 'w+') as f:
    f.write(s.getvalue())
    