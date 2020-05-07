#-*- coding: utf-8 -*-

import json
import copy

with open('train.json', 'r', encoding='utf-8') as fp:
	train_file = json.load(fp)

with open('dev.json', 'r', encoding='utf-8') as fp:
	dev_file = json.load(fp)

new_train_file = []
new_dev_file = []

for entry in train_file:
	entry['query'] = copy.copy(entry['sql_query'])
	entry.pop('sql_query')
	new_train_file.append(entry)

for entry in dev_file:
	entry['query'] = copy.copy(entry['sql_query'])
	entry.pop('sql_query')
	new_dev_file.append(entry)

with open('train_original.json', 'w', encoding='utf-8') as fp:
	json.dump(train_file, fp, indent=4, ensure_ascii=False)

with open('dev_original.json', 'w', encoding='utf-8') as fp:
	json.dump(dev_file, fp, indent=4, ensure_ascii=False)

with open('train.json', 'w', encoding='utf-8') as fp:
	json.dump(new_train_file, fp, indent=4, ensure_ascii=False)

with open('dev.json', 'w', encoding='utf-8') as fp:
	json.dump(new_dev_file, fp, indent=4, ensure_ascii=False)

'''
with open('db_schema.json', 'r', encoding='utf-8') as fp:
	file  = json.load(fp)

new_file = []

for db in file:
	db['table_names_original'] = copy.copy(db['table_names'])
	db['column_names_original'] = copy.copy(db['column_names'])
	#db['query'] = copy.copy(db['sql_query'])
	#db.pop('sql_query')
	new_file.append(db)

with open('db_schema_original.json', 'w', encoding='utf-8') as fp:
	json.dump(file, fp, indent=4, ensure_ascii=False)

with open('db_schema.json', 'w', encoding='utf-8') as fp:
	json.dump(new_file, fp, indent=4, ensure_ascii=False)
'''
