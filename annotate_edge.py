import json

with open('./spider/spider/tables_mod_withoutedge.json', 'r') as fp:
	databases = json.load(fp)

res_json = []

for db_idx, database in enumerate(databases):
	print('db_idx:	', db_idx)
	for tid, item in enumerate(database['table_names']):
		corresponding_columns = []
		for col in database['column_names']:
			if col[0] == tid:
				corresponding_columns.append(col[1])
		print('table name:	' + item, ',	', corresponding_columns)
	print("")
	for fk_idx, pair in enumerate(database['foreign_keys']):
		t1 = database['table_names'][database['column_names'][pair[0]][0]]
		t2 = database['table_names'][database['column_names'][pair[1]][0]]
		p1 = database['column_names'][pair[0]][1]
		p2 = database['column_names'][pair[1]][1]
		print("foreign_key %d" % fk_idx)
		print(t1+' . '+p1)
		print(t2+' . '+p2)
		print("")

	edge_tids = []

	while True:
		tname = input('which table is edge table?	')
		if tname == 'end':
			break
		edge_tid = None
		for tid, name in enumerate(database['table_names']):
			if name == tname:
				edge_tid = tid
				break
		if edge_tid is None:
			print("Table name not found! Please enter again")
			continue
		else:
			if edge_tid not in edge_tids:
				edge_tids.append(edge_tid)

	database['edge_tids'] = edge_tids
	res_json.append(database)

with open('./spider/spider/tables_mod.json', 'w') as fp:
	json.dump(res_json, fp, indent=4)
