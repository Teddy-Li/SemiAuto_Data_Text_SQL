import json

def find_from_list(db_id, lst):
	for idx, item in enumerate(lst):
		if item['db_id'] == db_id:
			return [idx, db_id]
	raise AssertionError

with open('./spider/spider/tables.json', 'r') as fp:
	dbs = json.load(fp)
with open('./spider/spider/train_spider.json', 'r') as fp:
	train_f = json.load(fp)
with open('./spider/spider/dev.json', 'r') as fp:
	dev_f = json.load(fp)

train_bucket = []
dev_bucket = []
non_bucket = []



for entry in train_f:
	if entry['db_id'] not in train_bucket:
		train_bucket.append(entry['db_id'])

for entry in dev_f:
	if entry['db_id'] not in dev_bucket:
		dev_bucket.append(entry['db_id'])

for entry in dbs:
	if entry['db_id'] not in train_bucket and entry['db_id'] not in dev_bucket:
		non_bucket.append(entry['db_id'])

for idx, item in enumerate(train_bucket):
	train_bucket[idx] = find_from_list(item, dbs)

for idx, item in enumerate(dev_bucket):
	dev_bucket[idx] = find_from_list(item, dbs)

for idx, item in enumerate(non_bucket):
	non_bucket[idx] = find_from_list(item, dbs)

dct = {'train': train_bucket, 'dev': dev_bucket, 'non': non_bucket}

with open('./spider/spider/table_bucket.json', 'w') as fp:
	json.dump(dct, fp)

