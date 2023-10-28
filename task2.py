from pyspark import SparkContext
import sys
import time
import os

filter_thresh = int(sys.argv[1])
support = sys.argv[2]
input_file_path = sys.argv[3]
output_file_path = sys.argv[4]

if os.path.exists('customer_product.csv'):
    os.remove('customer_product.csv')

def candidates_create(candidateset, size):
    candidate_itemsets = set()

    for i in range(len(candidateset)):
        for j in range(i + 1, len(candidateset)):
            first_set = candidateset[i]
            second_set = candidateset[j]
            if first_set[:size - 2] == second_set[:size - 2]:
                new_cand_set = tuple(sorted(set(first_set) | set(second_set)))
                if len(new_cand_set) == size:
                    candidate_itemsets.add(new_cand_set)

    return list(candidate_itemsets)

def count_itemset_subset(itemset):
    return sum(1 for x in grouped_data_RDD if set(itemset).issubset(x[1]))
sc = SparkContext('local[*]', 'tafeng')
sc.setLogLevel("ERROR") 
start = time.time()
# Processing original file
task2_rdd = sc.textFile(input_file_path)
header_row = task2_rdd.first()
def format_data(row):
    data = row.split(',')
    mmddyy = data[0].strip('"').split("/")
    formatted_date = "/".join([mmddyy[0], mmddyy[1], mmddyy[2][-2:]])
    customer_id = data[1].strip('"').lstrip('0')
    product_id = data[5].strip('"').lstrip('0')
    return formatted_date+'-'+customer_id, product_id
def create_inter_file(rows):
    with open('customer_product.csv', "a") as file:
        file.write(header+ '\n')
        for row in rows:
            file.write(f"{row[0]},{row[1]}\n")
    return iter([])

distinct_rdd = task2_rdd.filter(lambda x: not x.startswith('"TRANSACTION_DT"')).map(format_data)
formatted_rdd = distinct_rdd.distinct()

header = "DATE-CUSTOMER_ID,PRODUCT_ID"
csv_rdd = formatted_rdd.map(lambda x: (x[0], str(x[1])))
csv_rdd.coalesce(1).mapPartitions(create_inter_file).collect()

# Processing the data generated in intermediate file
updated_rdd = sc.textFile('customer_product.csv')
header_data = updated_rdd.first()
updated_rdd = updated_rdd.filter(lambda x: x != header_data)
cust_prod_rdd = updated_rdd.map(lambda line: line.split(",")).map(lambda parts: (parts[0], parts[1]))
#print(cust_prod_rdd.take(10))
customer_counts = cust_prod_rdd.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
#print(customer_counts.take(10))

filtered_rdd = customer_counts.filter(lambda x: x[1] > filter_thresh)
#print(filtered_rdd.take(10))
date_cust = set(filtered_rdd.map(lambda x: x[0]).collect())

# valid_customers_rdd = filtered_rdd.keys().map(lambda x: x[0])
updated_cust_prod_rdd = cust_prod_rdd.filter(lambda x: x[0] in date_cust).map(lambda x: [x[0], x[1]])
#print(updated_cust_prod_rdd.take(10))

## Task 1 code begins here

group_by_users = updated_cust_prod_rdd.map(lambda line: (line[0], set(line[1:])))
grouped = group_by_users.groupByKey().map(lambda a: (a[0], {item for biz in a[1] for item in biz}))

grouped_data_RDD = grouped.collect()
# print('grouped: ', grouped.take(10))
count_business_id = group_by_users.flatMap(lambda a: a[1]).map(lambda biz: (biz, 1)).reduceByKey(lambda a, b: a + b)
frequent_items_RDD = count_business_id.filter(lambda a: a[1] >= int(support)).map(lambda a: (a[0],)).sortBy(lambda a: a)
frequent_items = frequent_items_RDD.collect()
singletons = frequent_items

singles = count_business_id.map(lambda a: a[0]).sortBy(lambda a:a).collect()


index = 0
size = 2
length = len(frequent_items)
data_store = [frequent_items]
candidates = []
num_partitions = 5
while size <= length:
    get_candidates = candidates_create(frequent_items, size)
    frequent_RDD = sc.parallelize(get_candidates, numSlices=num_partitions)
    frequent_RDD.cache()
    candidates.append(frequent_RDD.collect())

    count_frequents = frequent_RDD.map(lambda itemset: (itemset, count_itemset_subset(itemset)))
    frequent_items_RDD = count_frequents.filter(lambda x: x[1] >= int(support)).map(lambda a: a[0])
    frequent_items = sorted(frequent_items_RDD.collect(), key=lambda x: tuple(sorted(x)))

    frequent_RDD.unpersist()
    if not frequent_items:
        break
    data_store.append(frequent_items)
    size += 1


formatted_data = []
for itemset_list in candidates:
    formatted_itemsets = ','.join([str(itemset) for itemset in itemset_list])
    formatted_data.append(f"{formatted_itemsets}")

formatted_output = '\n'.join(formatted_data)

with open(output_file_path, "w") as output_file:
    output_file.write("Candidates:\n")
    singles_formatted = [f'(\'{item}\')' for item in singles]
    singles_formatted = ','.join(singles_formatted)
    output_file.write(singles_formatted + '\n')
    output_file.write(formatted_output)
    output_file.write("\nFrequent Itemsets:\n")
    formatted_list = ','.join([f"('{item[0]}')" for item in singletons])
    output_file.write(formatted_list + '\n')
    for i, itemset in enumerate(data_store[1:], start=1):
        formatted_itemset = ','.join([str(tuple(x)) for x in itemset])
        output_file.write('\n'+formatted_itemset + '\n')

end = time.time()
print("Duration: ", end - start)
sc.stop()









