from pyspark import SparkContext
import sys
import time
from itertools import combinations

case_number = int(sys.argv[1])
support = sys.argv[2]
input_file_path = sys.argv[3]
output_file_path = sys.argv[4]

sc = SparkContext('local[*]', 'apriori')


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

start = time.time()

task1_RDD = sc.textFile(input_file_path)
header_row = task1_RDD.first()
data_RDD = task1_RDD.filter(lambda row: row != header_row).map(lambda line: line.split(','))

if case_number == 1:
    group_by_users = data_RDD.map(lambda line: (line[0], set(line[1:])))
    grouped = group_by_users.groupByKey().map(lambda a: (a[0], {item for biz in a[1] for item in biz}))
else:
    group_by_users = data_RDD.map(lambda line: (line[1], {line[0]}))
    grouped = group_by_users.groupByKey().map(lambda a: (a[0], {item for biz in a[1] for item in biz}))


grouped_data_RDD = grouped.collect()
# grouped_data_RDD = sc.broadcast(grouped_data)
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
num_partitions = 3
while size <= length:
    get_candidates = candidates_create(frequent_items, size)
    frequent_RDD = sc.parallelize(get_candidates, numSlices=num_partitions)
    frequent_RDD.cache()
    #result = frequent_RDD.collect()
    candidates.append(frequent_RDD.collect())

    count_frequents = frequent_RDD.map(lambda itemset: (itemset, count_itemset_subset(itemset)))
    # frequent_items_RDD = frequent_RDD.filter(lambda itemset: count_itemset_subset(itemset) >= int(support))

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
    output_file.write("Frequent Itemsets:\n")
    formatted_list = ','.join([f"('{item[0]}')" for item in singletons])
    output_file.write(formatted_list + '\n')
    for i, itemset in enumerate(data_store[1:], start=1):
        formatted_itemset = ','.join([str(tuple(x)) for x in itemset])
        output_file.write('\n'+formatted_itemset + '\n')

end = time.time()
print("Duration: ", end - start)
sc.stop()

