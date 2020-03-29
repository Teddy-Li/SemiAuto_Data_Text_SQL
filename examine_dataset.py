import copy
import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--data_path', type=str, default='./spider/spider/train_spider.json')
args = parser.parse_args()

with open(args.data_path, 'r') as fp:
	data = json.load(fp)

with open('./spider/spider/tables_mod.json', 'r') as fp:
	tables = json.load(fp)


# fp = open('train_gold.txt', 'w')
# for entry in data:
#	fp.write('final: ' + entry['final']['query']+'\n\n')
#	for turn in entry['interaction']:
#		fp.write(turn['query']+'\n')
#	fp.write('\n\n')
# fp.close()

def judge_valid(tables, pairs):
	validity = {}
	for tab in tables:
		validity[tab] = False
	for pair in pairs:
		if pair[0] in tables and pair[1] in tables:
			validity[pair[0]] = True
			validity[pair[1]] = True
	for key in validity:
		if validity[key] is False:
			return False
	return True


table_stats = {}
num_entries = {}

num_tables = {}
num_orderbys = {}
groupBy_numbers = {}
having_numbers = {}
number_of_tables_in_queried_props = {}
join_cond_cmper_numbers = {}
where_cond_numbers = {}
where_cond_cmpers = {}
orderby_dtypes = {}
subq_aggr_bucket = {}
aggr_nogroupby_bucket = {}
aggr_union_bucket = {}
num_of_props_queried_bucket = {}
dtypes_queried_bucket = {}
where_cond_cmpers_eachdtype = {}
queried_aggrs_eachdtype = {}
num_tables_in_subq_bucket = {}
limit_numbers_bucket = {}
having_countstar_numbers_bucket = {}
groupby_dtype_bucket = {}
column_dtype_bucket = {}
total_join_conditions = 0
num_bigturns_with_more_than_three_tables = 0
join_cond_not_fk_cnt = 0
join_cond_not_fk_not_same_cnt = 0
join_cond_moreorless_than_tableminus1_cnt = 0
orderby_without_limit_cnt = 0
orderby_asc_cnt = 0
orderby_desc_cnt = 0
where_subq_not_fk_cnt = 0
where_subq_not_fk_not_same_cnt = 0
where_and_having_both_present_cnt = 0
where_and_cnt = 0
where_or_cnt = 0
repeated_tables_cnt = 0
ordered_prop_selected_cnt = 0
ordered_prop_grouped_cnt = 0
ordered_prop_nothing_cnt = 0
where_cv_cnt = 0  # where column<->value(s) count
where_cc_cnt = 0  # where column<->column count
where_ci_cnt = 0  # where column<->in count
having_cond_cv_cnt = 0
having_cond_cc_cnt = 0
having_cond_ci_cnt = 0
having_with_star_cnt = 0
having_with_count_star_cnt = 0
having_with_count_star_cv = 0
orderby_star_cnt = 0
orderby_count_star_cnt = 0
num_of_columns_used_by_join = 0
num_of_columns_used_by_orderby = 0
num_of_columns_used_by_groupby = 0
num_of_columns_used_by_where = 0
num_of_columns_queried = 0
num_of_join_columns_queried = 0
num_of_orderby_columns_queried = 0
num_of_groupby_columns_queried = 0
num_of_where_columns_queried = 0
num_star_queried = 0
num_star_queried_with_count = 0
num_idcolumns_in_where = 0
num_subq_in_join_tables = 0
calculation_in_where_cnt = 0
distinct_query_cnt = 0
orderby_at_covered_cases_cnt = 0
like_contains_cnt = 0
like_startswith_cnt = 0
like_endswith_cnt = 0
same_column_multiple_wheres_cnt = 0
exist_groupby_cnt = 0
exist_groupby_selected_cnt = 0
groupby_selected_but_not_covered_cnt = 0

for idx, tab in enumerate(tables):
	if tab['db_id'] not in table_stats:
		table_stats[tab['db_id']] = {}
	table_stats[tab['db_id']]['num_tables'] = len(tab['table_names'])
	table_stats[tab['db_id']]['num_columns'] = len(tab['column_names'])
	table_stats[tab['db_id']]['num_fks'] = len(tab['foreign_keys'])
	for d in tab['column_types']:
		if d not in column_dtype_bucket:
			column_dtype_bucket[d] = 1
		else:
			column_dtype_bucket[d] += 1

for entry_id, entry in enumerate(data):
	if entry_id % 1000 == 0:
		print(entry_id)
	db_id = entry['db_id']
	if db_id not in num_entries:
		num_entries[db_id] = 1
	else:
		num_entries[db_id] += 1
	db = None
	for tab in tables:
		if tab['db_id'] == db_id:
			db = tab
	assert (db is not None)

	# about the number of tables present in a sub-query
	num = len(entry['sql']['from']['table_units'])
	if num > 3:
		'''
		print('query', turn['query'])
		print('utterance', turn['utterance'])
		print("")
		'''
		num_bigturns_with_more_than_three_tables += 1
	if num not in num_tables:
		num_tables[num] = 1
	else:
		num_tables[num] += 1

	tableunit_bucket = []
	for tab in entry['sql']['from']['table_units']:
		if tab[0] != 'table_unit':
			num_subq_in_join_tables += 1
		if tab[1] not in tableunit_bucket:
			tableunit_bucket.append(tab[1])
		else:
			repeated_tables_cnt += 1
	# print('repeated: ', entry['query'])

	coexistable_table_pairs = []
	columns_used_by_join = []

	if len(entry['sql']['from']['conds']) > 1:
		print(entry)

	# about the conditions used in joining the tables
	for cond in entry['sql']['from']['conds']:
		try:
			if cond == 'and':
				continue
			if cond[0] is not False:
				print(cond)
				raise AssertionError
			assert cond[2][0] == 0
			assert cond[2][1][0] == 0
			assert cond[2][2] is None
			assert cond[3][0] == 0
			assert cond[3][2] is False
			assert cond[4] is None
			idx1 = cond[2][1][1]
			idx2 = cond[3][1]
			cmper = cond[1]
			if idx1 not in columns_used_by_join:
				columns_used_by_join.append(idx1)
			if idx2 not in columns_used_by_join:
				columns_used_by_join.append(idx2)
			if cmper not in join_cond_cmper_numbers:
				join_cond_cmper_numbers[cmper] = 1
			else:
				join_cond_cmper_numbers[cmper] += 1
			total_join_conditions += 1
			if [idx1, idx2] not in db['foreign_keys'] and [idx2, idx1] not in db['foreign_keys']:
				join_cond_not_fk_cnt += 1
				if db['column_names'][idx1][1] != db['column_names'][idx2][1]:
					# print(db['column_names'][idx1], db['column_names'][idx2])
					join_cond_not_fk_not_same_cnt += 1
			else:
				coexistable_table_pairs.append([db['column_names'][idx1][0], db['column_names'][idx2][0]])
		except Exception as e:
			# print("!")
			# print(cond)
			raise
	num_of_columns_used_by_join += len(columns_used_by_join)

	# about whether number of join conditions is exactly one less than number of tables joined in the query
	if (len(entry['sql']['from']['conds']) - int((len(entry['sql']['from']['conds']) - 1) / 2) + 1) != len(
			entry['sql']['from']['table_units']) and len(entry['sql']['from']['table_units']) > 1:
		# print(len(entry['sql']['from']['conds']) - int((len(entry['sql']['from']['conds'])-1)/2) + 1 - len(entry[
		# 'sql']['from']['table_units'])) print("join: ", entry['query'])
		join_cond_moreorless_than_tableminus1_cnt += 1
	try:
		num_limit = entry['sql']['limit']
	except Exception as e:
		print(entry)
		print(e)
		raise
	if num_limit not in limit_numbers_bucket:
		limit_numbers_bucket[num_limit] = 1
	else:
		limit_numbers_bucket[num_limit] += 1

	has_orderby_star = False
	# about the columns used to order the returned result entries
	columns_used_by_orderby = []
	if len(entry['sql']['orderBy']) > 0:
		if entry['sql']['limit'] is None:
			orderby_without_limit_cnt += 1
		if entry['sql']['orderBy'][0] == 'asc':
			orderby_asc_cnt += 1
		elif entry['sql']['orderBy'][0] == 'desc':
			orderby_desc_cnt += 1
		assert (len(entry['sql']['orderBy']) == 2)
		orderbyn = len(entry['sql']['orderBy'][1])
		# if orderbyn > 1:
		#	print(turn['query'])
		if orderbyn not in num_orderbys:
			num_orderbys[orderbyn] = 1
		else:
			num_orderbys[orderbyn] += 1

		if entry['sql']['select'][0] != False:
			distinct_query_cnt += 1
		# print("distinct: ", entry['question'], '; ', entry['query'])
		assert (len(entry['sql']['select']) == 2)

		orderby_props = []
		selected_columns = []
		grouped_columns = []
		for p in entry['sql']['orderBy'][1]:
			orderby_props.append(p[1][1])
			if p[1][1] == 0:
				orderby_star_cnt += 1
				has_orderby_star = True
				if p[1][0] == 3:
					orderby_count_star_cnt += 1

		for p in entry['sql']['select'][1]:
			selected_columns.append(p[1][1][1])
		for p in entry['sql']['groupBy']:
			grouped_columns.append(p[1])
		for p in orderby_props:
			if p in selected_columns:
				ordered_prop_selected_cnt += 1
			elif p in grouped_columns:
				ordered_prop_grouped_cnt += 1
			else:
				ordered_prop_nothing_cnt += 1
		for pid in orderby_props:
			dtp = db['column_types'][pid]
			if dtp not in orderby_dtypes:
				orderby_dtypes[dtp] = 1
			else:
				orderby_dtypes[dtp] += 1
		columns_used_by_orderby = orderby_props

	num_of_columns_used_by_orderby += len(columns_used_by_orderby)

	if 'and' in entry['sql']['where'] and 'or' in entry['sql']['where']:
		# print(entry['query'])
		pass
	# about the where condition types
	columns_used_by_where = []
	for cond in entry['sql']['where']:
		if cond in ['and', 'or']:
			if cond == 'and':
				where_and_cnt += 1
			else:
				where_or_cnt += 1
			continue
		if cond[0] != False and cond[1] not in [8, 9]:
			print(entry)
			raise AssertionError

		assert cond[2][1][2] == False
		if cond[2][2] is not None:
			assert (cond[2][0] != 0)
			calculation_in_where_cnt += 1
		cmper = cond[1]
		if cmper not in where_cond_cmpers:
			where_cond_cmpers[cmper] = 1
		else:
			where_cond_cmpers[cmper] += 1
		if cmper == 8:
			assert (type(cond[3]) == type({}))
		if cmper == 9:
			_ = cond[3].strip('"')
			if _[0] == '%' and _[-1] == '%':
				like_contains_cnt += 1
			elif _[0] == '%':
				like_endswith_cnt += 1
			elif _[-1] == '%':
				like_startswith_cnt += 1
			# print(entry['query'], _)
			else:
				pass
		# if cmper == 1:
		#	print("between: ", cond)
		try:
			prop_dtype = db['column_types'][cond[2][1][1]]
		except Exception as e:
			print(db['db_id'], cond[2][1][1])
			raise
		if prop_dtype == 'datetime':
			# print(entry['query'])
			pass
		if prop_dtype not in where_cond_cmpers_eachdtype:
			where_cond_cmpers_eachdtype[prop_dtype] = {}
		if cmper not in where_cond_cmpers_eachdtype[prop_dtype]:
			where_cond_cmpers_eachdtype[prop_dtype][cmper] = 1
		else:
			where_cond_cmpers_eachdtype[prop_dtype][cmper] += 1
		if cond[2][1][1] not in columns_used_by_where:
			columns_used_by_where.append(cond[2][1][1])
		else:
			same_column_multiple_wheres_cnt += 1
		if db['column_types'][cond[2][1][1]] == 'id':
			num_idcolumns_in_where += 1
		tp = type(cond[3])
		if cmper == 8:
			assert isinstance(cond[3], dict)
		if type(cond[3]) in [type(''), type(0.0), type(0), type(u'')]:
			where_cv_cnt += 1
		elif isinstance(cond[3], list):
			where_cc_cnt += 1
			if cond[3][2] is not False:
				raise AssertionError
		elif isinstance(cond[3], dict):
			where_ci_cnt += 1
			subq = cond[3]
			num_tables_in_subq = len(subq['from']['table_units'])
			if num_tables_in_subq not in num_tables_in_subq_bucket:
				num_tables_in_subq_bucket[num_tables_in_subq] = 1
			else:
				num_tables_in_subq_bucket[num_tables_in_subq] += 1
			if len(subq['from']['table_units']) == 2:
				# print(entry)
				pass
			subq_aggr = subq['select'][1][0][0]
			if subq_aggr not in subq_aggr_bucket:
				subq_aggr_bucket[subq_aggr] = 1
			else:
				subq_aggr_bucket[subq_aggr] += 1
			assert (len(subq['select'][1]) == 1)
			assert (len(subq['select'][1][0]) == 2)
			'''
			if len(subq['orderBy']) != 0:
				ods = subq['orderBy'][1]
				if len(ods) > 1:
					print(entry)
					raise AssertionError
				else:
					if ods[0][1][1] not in [0, subq['select'][1][0][1][1][1]]:
						print(entry)
						raise AssertionError
			'''

			idx1 = cond[2][1][1]
			idx2 = subq['select'][1][0][1][1][1]
			if [idx1, idx2] not in db['foreign_keys'] and [idx2, idx1] not in db['foreign_keys']:
				where_subq_not_fk_cnt += 1
				if db['column_names'][idx1][1] != db['column_names'][idx2][1]:
					# print(db['column_names'][idx1], db['column_names'][idx2])
					where_subq_not_fk_not_same_cnt += 1
		else:
			raise AssertionError

	num_of_columns_used_by_where += len(columns_used_by_where)

	num_where_conditions = int((len(entry['sql']['where']) + 1) / 2)
	if num_where_conditions not in where_cond_numbers:
		where_cond_numbers[num_where_conditions] = 1
	else:
		where_cond_numbers[num_where_conditions] += 1
	if num_where_conditions == 3 and entry['sql']['where'][1] != entry['sql']['where'][3]:
		# print(entry)
		# print("")
		pass

	# about the columns to group by
	num_groupby = len(entry['sql']['groupBy'])
	if num_groupby > 0:
		exist_groupby_cnt += 1
	if num_groupby not in groupBy_numbers:
		groupBy_numbers[num_groupby] = 1
	else:
		groupBy_numbers[num_groupby] += 1

	columns_used_by_groupby = []
	for p in entry['sql']['groupBy']:
		assert p[0] == 0
		columns_used_by_groupby.append(p[1])
		assert (p[2] == False)
		gbdt = db['column_types'][p[1]]
		if gbdt not in groupby_dtype_bucket:
			groupby_dtype_bucket[gbdt] = 1
		else:
			groupby_dtype_bucket[gbdt] += 1

	num_of_columns_used_by_groupby += len(columns_used_by_groupby)

	# about the having conditions in group-by
	num_having = len(entry['sql']['having'])
	# if num_having == 3:
	#	print("having: ", entry['sql']['having'])
	if num_having not in having_numbers:
		having_numbers[num_having] = 1
	else:
		having_numbers[num_having] += 1

	if num_having > 0:
		for cond in entry['sql']['having']:
			if cond in ['and', 'or']:
				# print("Double having: ", entry['query'])
				continue
			having_col = cond[2][1][1]
			assert cond[0] is False
			assert cond[2][0] == 0
			if having_col == 0:
				having_with_star_cnt += 1
				if cond[2][1][0] == 3:
					having_with_count_star_cnt += 1
					if isinstance(cond[3], int) or isinstance(cond[3], float):
						having_with_count_star_cv += 1
						_val = int(cond[3])
						if int(cond[3]) not in having_countstar_numbers_bucket:
							having_countstar_numbers_bucket[_val] = 1
						else:
							having_countstar_numbers_bucket[_val] += 1
			if cond[1] == 9:
				# print("having: ", cond[3])
				pass
			if type(cond[3]) == type({}):
				having_cond_ci_cnt += 1
			elif type(cond[3]) == type([]):
				having_cond_cc_cnt += 1
			else:
				having_cond_cv_cnt += 1

	tables_in_queried_props = []
	num_of_props_queried = len(entry['sql']['select'][1])
	if num_of_props_queried not in num_of_props_queried_bucket:
		num_of_props_queried_bucket[num_of_props_queried] = 1
	else:
		num_of_props_queried_bucket[num_of_props_queried] += 1
	selected_subset_of_groupby = True
	have_grouped_by_column_selected = False
	unselected_groupby_columns = copy.copy(columns_used_by_groupby)
	# about the tables of properties queried
	for prop in entry['sql']['select'][1]:
		if prop[1][1][2] != False and prop[0] != 3:
			print(entry)
			raise AssertionError
		aggr_id = prop[0]
		prop_id = prop[1][1][1]
		if prop_id in unselected_groupby_columns:
			unselected_groupby_columns.remove(prop_id)
		if prop_id not in columns_used_by_groupby:
			selected_subset_of_groupby = False
		if db['column_types'][prop_id] not in queried_aggrs_eachdtype:
			queried_aggrs_eachdtype[db['column_types'][prop_id]] = {}
		if aggr_id not in queried_aggrs_eachdtype[db['column_types'][prop_id]]:
			queried_aggrs_eachdtype[db['column_types'][prop_id]][aggr_id] = 1
		else:
			queried_aggrs_eachdtype[db['column_types'][prop_id]][aggr_id] += 1

		if db['column_types'][prop_id] not in dtypes_queried_bucket:
			dtypes_queried_bucket[db['column_types'][prop_id]] = 1
		else:
			dtypes_queried_bucket[db['column_types'][prop_id]] += 1
		if prop_id == 0:
			num_star_queried += 1
			if prop[0] == 3:
				num_star_queried_with_count += 1
		tbid = db['column_names'][prop_id][0]
		if tbid not in tables_in_queried_props and tbid != -1:
			tables_in_queried_props.append(tbid)
		if num_groupby == 0:
			aggr_nogroupby = prop[0]
			if aggr_nogroupby not in aggr_nogroupby_bucket:
				aggr_nogroupby_bucket[aggr_nogroupby] = 1
			else:
				aggr_nogroupby_bucket[aggr_nogroupby] += 1
		num_of_columns_queried += 1
		if prop_id in columns_used_by_join:
			num_of_join_columns_queried += 1
		if prop_id in columns_used_by_orderby:
			num_of_orderby_columns_queried += 1
		if prop_id in columns_used_by_groupby:
			have_grouped_by_column_selected = True
			num_of_groupby_columns_queried += 1
		if prop_id in columns_used_by_where:
			num_of_where_columns_queried += 1
	if have_grouped_by_column_selected and len(unselected_groupby_columns) > 0:
		print(entry['query'])
		print(entry['question'])
		print("")
		groupby_selected_but_not_covered_cnt += 1
		pass
	elif have_grouped_by_column_selected:
		pass
		#raise AssertionError
	if selected_subset_of_groupby:
		if len(entry['sql']['orderBy']) > 0:
			orderby_at_covered_cases_cnt += 1
			if not has_orderby_star:
				# print("subset: ", db_id, ': ', entry['query'])
				pass
	if have_grouped_by_column_selected:
		exist_groupby_selected_cnt += 1
	# elif num_groupby > 0:
	#	print(entry)
	#	raise AssertionError

	if has_orderby_star and num_groupby == 0:
		# print(entry['query'])
		pass
	# if len(tables_in_queried_props) == 0:
	#	print("tables: ", entry)
	if len(tables_in_queried_props) not in number_of_tables_in_queried_props:
		number_of_tables_in_queried_props[len(tables_in_queried_props)] = 1
	else:
		number_of_tables_in_queried_props[len(tables_in_queried_props)] += 1

	# if len(tables_in_queried_props) > 1:
	#	if not judge_valid(tables_in_queried_props, coexistable_table_pairs):
	#		print(entry)
	#		raise AssertionError
	# about where-conditions and having-conditions being present at the same time
	if len(entry['sql']['where']) > 0 and len(entry['sql']['having']) > 0:
		where_and_having_both_present_cnt += 1
	# print("where and having both present: ", where_and_having_both_present_cnt)
	# print('query: ', entry['query'])
	# print('question: ', entry['question'])
	# print("")

	if entry['sql']['union'] is not None:
		assert len(entry['sql']['orderBy']) == 0
		if len(entry['sql']['union']['orderBy']) != 0:
			print(entry)
			raise AssertionError
		for prop in entry['sql']['select'][1]:
			aggr_union = prop[0]
			if aggr_union not in aggr_union_bucket:
				aggr_union_bucket[aggr_union] = 1
			else:
				aggr_union_bucket[aggr_union] += 1

	if entry['sql']['intersect'] is not None:
		if len(entry['sql']['orderBy']) != 0 or len(entry['sql']['intersect']['orderBy']) != 0:
			#print(entry)
			pass

	if entry['sql']['except'] is not None:
		if len(entry['sql']['orderBy']) != 0 or len(entry['sql']['except']['orderBy']) != 0:
			#print(entry)
			pass

total_entries = float(len(data))

print("")
print("total_join_conditions: ", total_join_conditions, format(total_join_conditions / total_entries, '.3f'))
print("join_cond_not_fk_cnt: ", join_cond_not_fk_cnt, format(join_cond_not_fk_cnt / total_entries, '.3f'))
print("join_cond_not_fk_not_same_cnt: ", join_cond_not_fk_not_same_cnt,
	  format(join_cond_not_fk_not_same_cnt / total_entries, '.3f'))
print("join_cond_moreorless_than_tableminus1_cnt: ", join_cond_moreorless_than_tableminus1_cnt,
	  format(join_cond_moreorless_than_tableminus1_cnt / total_entries, '.3f'))

print("total: ", len(data))
for num in num_tables:
	print(num, num_tables[num], format(num_tables[num] / total_entries, '.3f'))

print("categorized according to the number of order-bys: ")
ob_sum = 0
for num in num_orderbys:
	print(num, num_orderbys[num], format(num_orderbys[num] / total_entries, '.3f'))
	ob_sum += num_orderbys[num]
print("total number of order-byed sub-queries: ", ob_sum, format(ob_sum / total_entries, '.3f'))

print("categorized according to the number of group-bys: ")
gb_sum = 0
for num in groupBy_numbers:
	print(num, groupBy_numbers[num], format(groupBy_numbers[num] / total_entries, '.3f'))
	if num != 0:
		gb_sum += groupBy_numbers[num]
print("total number of group-byed sub-queries: ", gb_sum, format(gb_sum / total_entries, '.3f'))

print("categorized according to the number of having conditions: ")
hv_sum = 0
for num in having_numbers:
	print(num, having_numbers[num], format(having_numbers[num] / total_entries, '.3f'))
	if num != 0:
		hv_sum += having_numbers[num]
print("total number of sub-queries with having conditions: ", hv_sum, format(hv_sum / total_entries, '.3f'))
print("having_cond_cv_cnt: ", having_cond_cv_cnt, format(having_cond_cv_cnt / total_entries, '.3f'))
print("having_cond_cc_cnt: ", having_cond_cc_cnt, format(having_cond_cc_cnt / total_entries, '.3f'))
print("having_cond_ci_cnt: ", having_cond_ci_cnt, format(having_cond_ci_cnt / total_entries, '.3f'))
print("having_with_star_cnt: ", having_with_star_cnt, format(having_with_star_cnt / total_entries, '.3f'))
print("having_with_count_star_cnt: ", having_with_count_star_cnt,
	  format(having_with_count_star_cnt / total_entries, '.3f'))
print("having_with_count_star_cv: ", having_with_count_star_cv,
	  format(having_with_count_star_cv / total_entries, '.3f'))
print("orderby_star_cnt: ", orderby_star_cnt, format(orderby_star_cnt / total_entries, '.3f'))
print("orderby_count_star_cnt: ", orderby_count_star_cnt, format(orderby_count_star_cnt / total_entries, '.3f'))
print("")

print("number_of_tables_in_queried_props: ")
for num in number_of_tables_in_queried_props:
	print(num, ': ', number_of_tables_in_queried_props[num],
		  format(number_of_tables_in_queried_props[num] / total_entries, '.3f'))
print("")

print("join_cond_cmper_numbers: ")
for num in join_cond_cmper_numbers:
	print('	', num, join_cond_cmper_numbers[num], format(join_cond_cmper_numbers[num] / total_entries, '.3f'))
print("")

print("where_cond_numbers: ")
for num in where_cond_numbers:
	print(num, where_cond_numbers[num], format(where_cond_numbers[num] / total_entries, '.3f'))
print("")

print("where_cond_cmpers: ")
for num in where_cond_cmpers:
	print(num, where_cond_cmpers[num], format(where_cond_cmpers[num] / total_entries, '.3f'))
print("")

print("orderby_dtypes: ")
for dtype in orderby_dtypes:
	print(dtype, orderby_dtypes[dtype], format(orderby_dtypes[dtype] / total_entries, '.3f'))
print("")

print("subq_aggr_bucket: ")
for idx in subq_aggr_bucket:
	print(idx, subq_aggr_bucket[idx], format(subq_aggr_bucket[idx] / total_entries, '.3f'))
print("")

print("aggr_nogroupby_bucket: ")
for idx in aggr_nogroupby_bucket:
	print(idx, aggr_nogroupby_bucket[idx], format(aggr_nogroupby_bucket[idx] / total_entries, '.3f'))
print("")

print("aggr_union_bucket: ")
for idx in aggr_union_bucket:
	print(idx, aggr_union_bucket[idx], format(aggr_union_bucket[idx] / total_entries, '.3f'))
print("")

print("num_of_props_queried_bucket: ")
for idx in num_of_props_queried_bucket:
	print(idx, num_of_props_queried_bucket[idx], format(num_of_props_queried_bucket[idx] / total_entries, '.3f'))
print("")

print("dtypes_queried_bucket: ")
for idx in dtypes_queried_bucket:
	print(idx, dtypes_queried_bucket[idx], format(dtypes_queried_bucket[idx] / total_entries, '.3f'))
print("")

print("where_cond_cmpers_eachdtype: ")
for idx in where_cond_cmpers_eachdtype:
	print(idx, ': ', where_cond_cmpers_eachdtype[idx])
print("")

print("queried_aggrs_eachdtype: ")
for idx in queried_aggrs_eachdtype:
	print(idx, ": ", queried_aggrs_eachdtype[idx])
print("")

print("num_tables_in_subq_bucket: ")
for idx in num_tables_in_subq_bucket:
	print(idx, ": ", num_tables_in_subq_bucket[idx], format(num_tables_in_subq_bucket[idx] / total_entries, '.3f'))
print("")

print("limit_numbers_bucket: ")
for idx in limit_numbers_bucket:
	print(idx, ": ", limit_numbers_bucket[idx], format(limit_numbers_bucket[idx] / total_entries, '.3f'))
print("")

print("having_countstar_numbers_bucket: ")
for idx in having_countstar_numbers_bucket:
	print(idx, ": ", having_countstar_numbers_bucket[idx],
		  format(having_countstar_numbers_bucket[idx] / total_entries, '.3f'))
print("")

print("column_dtype_bucket: ")
for idx in column_dtype_bucket:
	print(idx, "; ", column_dtype_bucket[idx])
print("")

print('groupby_dtype_bucket: ')
for idx in groupby_dtype_bucket:
	print(idx, "; ", groupby_dtype_bucket[idx], format(groupby_dtype_bucket[idx] / total_entries, '.3f'))
print("")

print("orderby_without_limit_cnt: ", orderby_without_limit_cnt,
	  format(orderby_without_limit_cnt / total_entries, '.3f'))
print("orderby_asc_cnt: ", orderby_asc_cnt, format(orderby_asc_cnt / total_entries, '.3f'))
print("orderby_desc_cnt: ", orderby_desc_cnt, format(orderby_desc_cnt / total_entries, '.3f'))
print("")
print("where_cnt_total: ", where_cv_cnt + where_cc_cnt + where_ci_cnt,
	  format((where_cv_cnt + where_cc_cnt + where_ci_cnt) / total_entries, '.3f'))
print("where_cv_cnt: ", where_cv_cnt, format(where_cv_cnt / total_entries, '.3f'))
print("where_cc_cnt: ", where_cc_cnt, format(where_cc_cnt / total_entries, '.3f'))
print("where_ci_cnt: ", where_ci_cnt, format(where_ci_cnt / total_entries, '.3f'))
print("where_subq_not_fk_cnt: ", where_subq_not_fk_cnt, format(where_subq_not_fk_cnt / total_entries, '.3f'))
print("where_subq_not_fk_not_same_cnt: ", where_subq_not_fk_not_same_cnt,
	  format(where_subq_not_fk_not_same_cnt / total_entries, '.3f'))
print("where_and_having_both_present_cnt: ", where_and_having_both_present_cnt,
	  format(where_and_having_both_present_cnt / total_entries, '.3f'))
print("where_and_cnt: ", where_and_cnt, format(where_and_cnt / total_entries, '.3f'))
print("where_or_cnt: ", where_or_cnt, format(where_or_cnt / total_entries, '.3f'))

print("ordered_prop_selected_cnt: ", ordered_prop_selected_cnt,
	  format(ordered_prop_selected_cnt / total_entries, '.3f'))
print("ordered_prop_grouped_cnt: ", ordered_prop_grouped_cnt, format(ordered_prop_grouped_cnt / total_entries, '.3f'))
print("ordered_prop_nothing_cnt: ", ordered_prop_nothing_cnt, format(ordered_prop_nothing_cnt / total_entries, '.3f'))

print("repeated_tables_cnt: ", repeated_tables_cnt, format(repeated_tables_cnt / total_entries, '.3f'))

print("num_bigturns_with_more_than_three_tables: ", num_bigturns_with_more_than_three_tables,
	  format(num_bigturns_with_more_than_three_tables / total_entries, '.3f'))

print("")
print("num_of_columns_queried: ", num_of_columns_queried, format(num_of_columns_queried / total_entries, '.3f'))
print("num_of_columns_used_by_join: ", num_of_columns_used_by_join,
	  format(num_of_columns_used_by_join / total_entries, '.3f'))
print("num_of_join_columns_queried: ", num_of_join_columns_queried,
	  format(num_of_join_columns_queried / total_entries, '.3f'))
print("num_of_columns_used_by_orderby: ", num_of_columns_used_by_orderby,
	  format(num_of_columns_used_by_orderby / total_entries, '.3f'))
print("num_of_orderby_columns_queried: ", num_of_orderby_columns_queried,
	  format(num_of_orderby_columns_queried / total_entries, '.3f'))
print("num_of_columns_used_by_groupby: ", num_of_columns_used_by_groupby,
	  format(num_of_columns_used_by_groupby / total_entries, '.3f'))
print("num_of_groupby_columns_queried: ", num_of_groupby_columns_queried,
	  format(num_of_groupby_columns_queried / total_entries, '.3f'))
print("num_of_columns_used_by_where: ", num_of_columns_used_by_where,
	  format(num_of_columns_used_by_where / total_entries, '.3f'))
print("num_of_where_columns_queried: ", num_of_where_columns_queried,
	  format(num_of_where_columns_queried / total_entries, '.3f'))
print("")
print("num_star_queried: ", num_star_queried, format(num_star_queried / total_entries, '.3f'))
print("num_star_queried_with_count: ", num_star_queried_with_count,
	  format(num_star_queried_with_count / total_entries, '.3f'))
print("num_idcolumns_in_where: ", num_idcolumns_in_where, format(num_idcolumns_in_where / total_entries, '.3f'))
print("num_subq_in_join_tables: ", num_subq_in_join_tables, format(num_subq_in_join_tables / total_entries, '.3f'))
print("calculation_in_where_cnt: ", calculation_in_where_cnt, format(calculation_in_where_cnt / total_entries, '.3f'))
print("distinct_query_cnt: ", distinct_query_cnt, format(distinct_query_cnt / total_entries, '.3f'))
print("orderby_at_covered_cases_cnt: ", orderby_at_covered_cases_cnt,
	  format(orderby_at_covered_cases_cnt / total_entries, '.3f'))
print("like_contains_cnt: ", like_contains_cnt, format(like_contains_cnt / total_entries, '.3f'))
print("like_startswith_cnt: ", like_startswith_cnt, format(like_startswith_cnt / total_entries, '.3f'))
print("like_endswith_cnt: ", like_endswith_cnt, format(like_endswith_cnt / total_entries, '.3f'))
print("same_column_multiple_wheres_cnt: ", same_column_multiple_wheres_cnt,
	  format(same_column_multiple_wheres_cnt / total_entries, '.3f'))
print("exist_groupby_cnt: ", exist_groupby_cnt)
print("exist_groupby_selected_cnt: ", exist_groupby_selected_cnt)
print("groupby_selected_but_not_covered_cnt: ", groupby_selected_but_not_covered_cnt)

for key in table_stats:
	if key not in num_entries:
		n = 0
	else:
		n = num_entries[key]
	table_stats[key]['num_queries'] = n
with open('./spider/spider/table_stats_for_num_of_queries.json', 'w') as fp:
	json.dump(table_stats, fp, indent=4)
