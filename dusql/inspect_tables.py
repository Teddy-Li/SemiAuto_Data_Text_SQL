import json
import sqlite3

db_path = './databases'

with open('db_schema.json', 'r') as fp:
	file = json.load(fp)

dtypes = []

for entry in file:
	for d in entry['column_types']:
		if d not in dtypes:
			dtypes.append(d)

print(dtypes)

newfile = []

for ent_idx, entry in enumerate(file):
	print("db_id: ", entry['db_id'])
	print("")
	dbname = entry['db_id']
	conn = sqlite3.connect(db_path+'/%s.db'%dbname)
	crsr = conn.cursor()
	prop_ori_names = {}

	for col in entry['column_names']:
		if col[1] not in prop_ori_names:
			prop_ori_names[col[1]] = 1
		else:
			prop_ori_names[col[1]] += 1

	#for key in prop_ori_names:
	#	if prop_ori_names[key] > 1:
	#		print(key)

	print("Table names:")
	for tab in entry['table_names']:
		print(tab)

	# skip the '*' column
	for colidx, col in enumerate(entry['column_names']):
		if colidx == 0:
			entry['column_types'][0] = 'varchar(1000)'
			continue
		tabname = entry['table_names'][col[0]]
		colname = col[1]
		sql = 'select [%s] from [%s]' % (colname, tabname)
		answer = crsr.execute(sql).fetchall()
		tp = entry['column_types'][colidx]
		if 'id' in colname:
			entry['column_types'][colidx] = 'id'
		elif tp == 'text':
			entry['column_types'][colidx] = 'varchar(1000)'
		elif tp == 'time':
			if '年' in colname:
				entry['column_types'][colidx] = 'year'
			else:
				entry['column_types'][colidx] = 'datetime'
		elif tp == 'binary':
			entry['column_types'][colidx] = 'bit'
		elif tp == 'number':
			for a in answer:
				if isinstance(a[0], str):
					a = a[0].strip('%万公司平方米公里单首家千百克天月周小时分钟秒年亿元人吨个kmg')
				else:
					a = a[0]
				try:
					if (float(a) - int(float(a))) < 0.0000001:
						entry['column_types'][colidx] = 'int'
					else:
						entry['column_types'][colidx] = 'double'
						break  # column is double if one of the values cannot be expressed as 'int'
				except ValueError as e:
					entry['column_types'][colidx] = 'varchar(1000)'
					print('text: ', a)
					break
		else:
			raise AssertionError

	newfile.append(entry)

with open('tables_mod.json', 'w', encoding='utf-8') as fp:
	json.dump(newfile, fp, ensure_ascii=False, indent=4)



