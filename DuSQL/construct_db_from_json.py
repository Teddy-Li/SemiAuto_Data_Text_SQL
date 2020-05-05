#-*- coding: utf-8 -*-

import json
import sqlite3
import pandas

with open('../db_schema.json', 'r') as fp:
	schema = json.load(fp)

with open('../db_content.json', 'r') as fp:
	content = json.load(fp)

db_infos = {}  # id: (schema, content)

for sch in schema:
	dbid = sch['db_id']
	found = False
	for item in content:
		if item['db_id'] == dbid:
			db_infos[dbid] = (sch, item)
			found = True
			break
	if not found:
		raise AssertionError

revised_schema = []
revised_content = []

for kidx, key in enumerate(db_infos):
	(sch, con) = db_infos[key]
	dbname = key+'.db'
	try:
		connect = sqlite3.connect(dbname)
	except Exception as e:
		print(dbname)
		print(e)
		raise
	print("")
	print("Created database %s" % key)
	for tid, tname in enumerate(sch['table_names']):
		ori_tname = tname
		if tname[0] in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
			tname = '['+tname+']'
			print("Table starting with number!")
			#sch['table_names'][tid] = tname
		crsr = connect.cursor()
		sql = " CREATE TABLE " + tname + '( '
		cols = []  # (name, )
		pids = []
		for cid, item in enumerate(sch['column_names']):
			if item[0] == tid:
				if item[1][0] in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'] or '(' in item[1]:
					item[1] = '[' + item[1] + ']'
					sch['column_names'][cid][1] = item[1]
				cur_col = item[1] + ' ' + sch['column_types'][cid]
				#if cid in sch['primary_keys']:
				#	cur_col += ' PRIMARY KEY '
				cols.append(cur_col)
				pids.append(cid)

		# wash out the replicated columns (remember to also wash out those in Content)
		temp_cols = []
		temp_pids = []
		present_flags = []
		for _ord, col in enumerate(cols):
			if col not in cols[:_ord]:
				temp_cols.append(col)
				temp_pids.append(pids[_ord])
				present_flags.append(True)
			else:
				present_flags.append(False)
		cols = temp_cols
		pids = temp_pids

		sql += ', '.join(cols)
		for pid in pids:
			for pair in sch['foreign_keys']:
				if pid == pair[0]:
					sql += ', FOREIGN KEY(%s) REFERENCES %s(%s) ' % (sch['column_names'][pid][1],
																  sch['table_names'][sch['column_names'][pair[1]][0]],
																  sch['column_names'][pair[1]][1])
					break
		sql += ')'
		try:
			crsr.execute(sql)
		except sqlite3.OperationalError as e:
			print(sql)
			print("Error!")
			raise
		crsr.close()
		connect.commit()
		print("Created table %s" % tname)
		crsr = connect.cursor()

		table_content = None
		for nm in con['tables']:
			assert nm == con['tables'][nm]['table_name']
			if nm == ori_tname:
				table_content = con['tables'][nm]
				break

		if table_content is None:
			raise AssertionError

		for data_entry in table_content['cell']:
			sql = "INSERT INTO %s VALUES (" % tname
			data_strs = [('"'+str(d)+'"') for d in data_entry]
			assert len(present_flags) == len(data_strs)

			washed_data_strs = []
			for dord, item in enumerate(data_strs):
				if present_flags[dord] is True:
					washed_data_strs.append(item)
				else:
					print("!")

			sql += ', '.join(washed_data_strs)
			sql += ')'
			crsr.execute(sql)
		crsr.close()
		connect.commit()
		print("Added values for %s" % tname)
	#revised_schema.append(sch)
	#revised_content.append(con)







