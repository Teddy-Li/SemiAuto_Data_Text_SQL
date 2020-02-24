import json
import sqlite3

db_path = '/Users/teddy/Files/spider/spider/database/'

with open("tables.json", 'r') as fp:
	file = json.load(fp)

dtypes = {}

for ent_idx, entry in enumerate(file):
	dbname = entry['db_id']
	conn = sqlite3.connect(db_path+dbname+'/%s.sqlite' % dbname)
	crsr = conn.cursor()
	types = ['varchar(1000)']
	prop_ori_names = {}

	for col in entry['column_names_original']:
		if col[1] not in prop_ori_names:
			prop_ori_names[col[1]] = 1
		else:
			prop_ori_names[col[1]] += 1
	
	for key in prop_ori_names:
		if prop_ori_names[key] > 1:
			print(prop_ori_names)

	for tab in entry['table_names_original']:
		line = 'pragma table_info(%s)' % ('['+tab+']')
		ans = crsr.execute(line).fetchall()


		for idx, col in enumerate(ans):
			col_name = col[1]
			col_name = col_name.lower()
			tp = col[2]
			tp = tp.lower()

			if 'date' == tp:
				tp = 'datetime'
			elif 'numeric' in tp:
				if '0' == tp[-2] or tp == 'numeric':
					tp = 'int'
				else:
					tp = 'double'
			elif 'char' in tp and 'var' not in tp: 
				tp = 'varchar(1000)'
			elif 'varchar' in tp:
				tp = 'varchar(1000)'
			elif len(tp) == 0:
				tp = 'varchar(1000)'
			elif 'text' == tp:
				tp = 'varchar(1000)'
			elif 'decimal' in tp or 'real' in tp or 'float' in tp:
				tp = 'double'
			elif 'number' in tp or 'int' in tp:
				tp = 'int'
			elif 'bool' in tp:
				tp = 'bool'

			# check out the types of values in this column, if all of them are integers, change the type to 'int'
			if tp in ['int', 'double']:
				v_line = 'select %s from %s' % (col_name, '['+tab+']')
				values = crsr.execute(v_line).fetchall()
				is_int = True
				for v in values:
					assert isinstance(v, tuple)
					assert len(v) == 1
					v = v[0]
					if v is None or v == '' or v == 'inf' or v == 'nil' or v == 'NULL':
						continue
					try:
						if int(v) != v:
							is_int = False
					except Exception as e:
						print("!!!")
						print(v)
						print("!	", dbname, '; ', tab, '; ', col_name)
						break
				if is_int:
					tp = 'int'
				else:
					if tp == 'int':
						print(dbname, '; ', tab, '; ', col_name)
					tp = 'double'


			if '_id' in col_name or col_name == 'id':
				print(col)
				#if 'numeric' not in tp:
				#	print('type 1 error: ', col)
				#	raise AssertionError
				#elif '0' != tp[-2] and tp != 'numeric' and 'int' not in tp:
				#	print('type 2 error: ', col)
				#	raise AssertionError
				if 'varchar' not in tp:
					tp = 'id'

			types.append(tp)
			if tp not in dtypes:
				dtypes[tp] = 1
			else:
				dtypes[tp] += 1
	file[ent_idx]['column_types'] = types
	crsr.close()
	conn.close()

for key in dtypes:
	print(key, ': ', dtypes[key])
with open('/Users/teddy/Files/spider/spider/tables_mod.jsonl', 'w') as fp:
	json.dump(file, fp, indent=4)

'''
if 'date' == tp:
				tp = 'datetime'
			if 'numeric' in tp:
				if '0' == tp[-2] or tp == 'numeric':
					tp = 'int'
				else:
					tp = 'double'
			if 'char' in tp or len(tp) == 0 or tp == 'text': 
				tp = 'varchar'
			if 'decimal' in tp or 'real' in tp or 'float' in tp:
				tp = 'double'
			if 'number' in tp or 'int' in tp:
				tp = 'int'
			if 'bool' in tp or 'bit' == tp:
				tp = 'bool'
			if 'year' == tp:
				tp = 'int'






dtypes = []
column_names_with_spaces = []
table_names_with_spaces = []

print(len(file))
for entry in file:
	for idx, tp in enumerate(entry['column_types']):
		if tp not in dtypes:
			dtypes.append(tp)
		if tp == 'time':
			print(entry['db_id'])
	for nm in entry['column_names_original']:
		if ' ' in nm[1]:
			column_names_with_spaces.append(nm)
	for nm in entry['table_names_original']:
		if ' 'in nm[1]:
			table_names_with_spaces.append(nm)

print(dtypes)
print("")
print("columns: ")
print(column_names_with_spaces)
print("")
print("tables: ")
print(table_names_with_spaces)
'''