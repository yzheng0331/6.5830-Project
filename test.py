import dask.dataframe as dd
import time

df1 = dd.read_csv('./dbgen/customer.csv')
df2 = dd.read_csv('./dbgen/order.csv')

start_time = time.time()
merged_df = dd.merge(df1, df2, on='common_column')
merged_df = merged_df.compute()  # This triggers the computation and waits for it to finish
end_time = time.time()

# Step 4: Measure Time
elapsed_time = end_time - start_time
print(f"Merge operation took {elapsed_time} seconds.")

