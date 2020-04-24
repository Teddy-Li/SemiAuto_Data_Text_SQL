from phrase_structures import *
from phrase_structures import _uniquecount, _count_uniqueness_specified


CMPERS = [CMP(' is between {0[0]} and {0[1]}', '在{0[0]}和{0[1]}之间', ' between {0[0]} and {0[1]}', 1),
		  CMP(' is equal to {0}', '是{0}', ' = {0}', 2), CMP(' is larger than {0}', '比{0}大', ' > {0}', 3),
		  CMP(' is smaller than {0}', '比{0}小', ' < {0}', 4), CMP(' is not smaller than {0}', '不比{0}小', ' >= {0}', 5),
		  CMP(' is not larger than {0}', '不比{0}大', ' <= {0}', 6), CMP(' is not {0}', '不等于{0}', ' != {0}', 7),
		  CMP(' is among {0}', '在{0}之中', ' in {0}', 8), CMP('{0}', '{0}', ' like {0}', 9)]

# these distributions are naturally proportional
num_tables_distribution = {False: transform2distribution_proportional(numpy.array([4361., 3200., 22., 6.])),
						   True: transform2distribution_proportional(numpy.array([459., 51., 1., 0.01]))}
num_wheres_distribution = {False: transform2distribution_proportional(numpy.array([3300., 2500., 900., 400, 1.])),
						   True: transform2distribution_proportional(numpy.array([4., 1.5, 0.2,
																				  0.01,
																				  0.]))}  # padded each number with 10 to allow extreme queries with 4 where-conditions
num_selected_distribution = transform2distribution_proportional(numpy.array([0., 4800., 1700., 350., 40.]))

AGGR_SUBQDIST = transform2distribution_proportional(
	numpy.array([310., 66., 44., 0., 0., 90.]))  # only allow max, min, avg


def is_simple_query(qrynp):
	phrase = qrynp.np
	if len(phrase.table_ids) > 2:
		return False
	if len(phrase.queried_props) > 3:
		return False
	for cdt in phrase.cdts:
		if isinstance(cdt.right, dict):
			return False
	if len(phrase.group_props) > 1:
		return False
	if len(phrase.having_cdts) > 0:
		return False
	if len(phrase.orderby_props) > 1:
		return False
	if phrase.np_2 is not None:
		return False
	return True


def fetch_join_rel(new_tid, table_ids, prop_rels, typenps, propertynps, allow_pseudo_rels=False):
	relevant_prop_rels = []
	for idx, rel in enumerate(prop_rels):
		# allow only those relations whose types are both used in query
		if rel.table_ids[0] in table_ids and rel.table_ids[1] == new_tid:
			relevant_prop_rels.append(rel)
		elif rel.table_ids[0] == new_tid and rel.table_ids[1] in table_ids:
			relevant_prop_rels.append(rel)
	# if there are actual relations
	if len(relevant_prop_rels) > 0:
		scores = []
		for rel in relevant_prop_rels:
			scores.append(rel.score)
		scores = numpy.array(scores)
		dist = transform2distribution(scores)
		try:
			fetched_rel = numpy.random.choice(relevant_prop_rels, p=dist)
		except Exception as e:
			print(e)
			raise
	# if no actual relations exist
	elif allow_pseudo_rels:
		left_available_propids = typenps[new_tid].properties
		right_available_propids = []
		for tid in table_ids:
			right_available_propids += typenps[tid].properties
		pseudo_rels = []
		for pid1 in left_available_propids:
			for pid2 in right_available_propids:
				if propertynps[pid1].dtype == propertynps[pid2].dtype:
					pseudo_rels.append([pid1, pid2])
		fetched_rel = random.choice(pseudo_rels)
		fetched_rel = PROP_REL(fetched_rel[0], fetched_rel[1], propertynps[fetched_rel[0]].table_id,
							   propertynps[fetched_rel[1]].table_id, 1)
	else:
		raise AssertionError

	cmper = copy.deepcopy(CMPERS[1])  # always use equality comparer
	c_english = propertynps[fetched_rel.prop_ids[0]].c_english.format(
		typenps[fetched_rel.table_ids[0]].c_english + '\'s ') + cmper.c_english.format(
		propertynps[fetched_rel.prop_ids[1]].c_english.format(typenps[fetched_rel.table_ids[1]].c_english + '\'s '))
	c_chinese = propertynps[fetched_rel.prop_ids[0]].c_chinese.format(
		typenps[fetched_rel.table_ids[0]].c_chinese + '的') + cmper.c_chinese.format(
		propertynps[fetched_rel.prop_ids[1]].c_chinese.format(typenps[fetched_rel.table_ids[1]].c_chinese + '的'))
	c_english_half = cmper.c_english.format(
		propertynps[fetched_rel.prop_ids[1]].c_english.format(typenps[fetched_rel.table_ids[1]].c_english + '\'s '))

	z = propertynps[fetched_rel.prop_ids[0]].z + cmper.z.format(propertynps[fetched_rel.prop_ids[1]].z)
	left = copy.deepcopy(propertynps[fetched_rel.prop_ids[0]])
	right = copy.deepcopy(propertynps[fetched_rel.prop_ids[1]])
	fetched_cdt = CDT(c_english, c_chinese, z, left, right, cmper)

	return fetched_cdt


# 'in' comparer in where conditions appear only in those occasions with c-i conditions
#
# use_aggr_for_left_prop is equal to true iff it is a having condition
def construct_cv_where_cdt(available_prop_ids, propertynps, use_aggr_for_left_prop=False, assigned_prop=None,
						   no_negative=False, type_to_count=None):
	if assigned_prop is None:
		# the left-value in where conditions cannot have aggregators
		scores = [0] * len(available_prop_ids)
		for idx, prop_id in enumerate(available_prop_ids):
			if propertynps[prop_id].dtype in UNLIKELY_COND_DTYPES:
				scores[idx] = -10000
			elif propertynps[prop_id].dtype == 'id':
				scores[idx] *= 0.3
		# explicitly use softmax to convert into a distribution
		scores = transform2distribution_softmax(numpy.array(scores))
		chosen_prop_id = numpy.random.choice(available_prop_ids, p=scores)
		chosen_prop = copy.deepcopy(propertynps[chosen_prop_id])
		if use_aggr_for_left_prop:
			# assign different distributions for aggregators according to the property dtypes
			aggr_id = numpy.random.choice(numpy.arange(len(AGGREGATION_FUNCTIONS)), p=AGGR_DISTS[chosen_prop.dtype])
			chosen_prop.set_aggr(aggr_id)
	else:
		chosen_prop = assigned_prop

	# if there are single2multiple foreign key relationships between the multiple tables, the
	if chosen_prop.dtype == 'star' and chosen_prop.aggr == 3 and type_to_count is not None:
		assert isinstance(type_to_count, TYPENP)
		chosen_prop.c_english = ' the number of %s ' % type_to_count.c_english
		chosen_prop.c_chinese = '%s的数量' % type_to_count.c_chinese

	if chosen_prop.aggr == 3:
		assert (AGGREGATION_FUNCTIONS[3] == _uniquecount)
		available_values = []
	else:
		available_values = chosen_prop.values

	# for 'COUNT(...)' clauses, '<' and '>' comparison operators are allowed, just as for 'int'
	if chosen_prop.aggr == 3:
		cmper_distribution = numpy.array(CMP_DISTS['int'])
	else:
		cmper_distribution = numpy.array(CMP_DISTS[chosen_prop.dtype])
	assert (len(cmper_distribution) == 9)
	cmper_distribution[7] = 0
	cmper_distribution = transform2distribution_proportional(cmper_distribution)

	cmper = copy.deepcopy(numpy.random.choice(CMPERS, p=cmper_distribution))

	if cmper.index == 1:
		num_values = 2
	else:
		num_values = 1
	if cmper.index == 2 and chosen_prop.dtype in ['int', 'double']:
		cmper.c_english = ' is equal to {0}'
		cmper.c_chinese = '与{0}相等'

	if available_values is None:
		raise AssertionError

	# print("num_values: ", num_values)
	if chosen_prop.aggr != 3:
		value = copy.deepcopy(
			numpy.random.choice(available_values, num_values,
								replace=False))  # object 'value' is list of either length 1 or length 2
		if cmper.index == 9:
			assert len(value) == 1
			v = value[0].z
			rho = random.random()
			if rho < 0.8:
				v = cut_value_for_like(v, mode='mid')
				cmper.set_mode('mid')
			elif rho < 0.95:
				v = cut_value_for_like(v, mode='head')
				cmper.set_mode('head')
			else:
				v = cut_value_for_like(v, mode='tail')
				cmper.set_mode('tail')
			value = [VALUENP(v.strip('%'), v.strip('%'), v, value[0].dtype)]
	else:
		count_dist = [0, 100, 150, 40]
		count_dist += [0.1] * 97
		count_dist[5] += 5
		for idx in range(10, 101, 10):
			count_dist[idx] += 0.4
		count_dist = transform2distribution_proportional(count_dist)
		value = random.choices(range(101), k=num_values, weights=count_dist)
		for i in range(len(value)):
			value[i] = VALUENP(str(value[i]), str(value[i]), str(value[i]), 'int')

	# set negative is moved below 'set_mode' in order to avoid 'starts with not xxx'
	if cmper.index in [8, 9]:
		rho = random.random()
		if rho < 0.1 and not no_negative:
			cmper.set_negative()

	if num_values == 1:
		if not isinstance(value[0], VALUENP):
			raise AssertionError
		value_ce = value[0].c_english
		value_cc = value[0].c_chinese
		value_z = '#' + value[0].z + '#'
	else:
		value_ce = []
		value_cc = []
		value_z = []
		if value is None:
			raise AssertionError
		value.sort()  # 'between' requires that the two values are in ascending order
		if value is None:
			raise AssertionError
		for item in value:
			value_ce.append(item.c_english)
			value_cc.append(item.c_chinese)
			value_z.append('#' + item.z + '#')

	c_english = chosen_prop.c_english.format('') + cmper.c_english.format(value_ce)
	c_chinese = chosen_prop.c_chinese.format('') + cmper.c_chinese.format(value_cc)
	z = chosen_prop.z + ' ' + cmper.z.format(value_z)
	cdt = CDT(c_english, c_chinese, z, chosen_prop, value, cmper)
	return cdt


def construct_cc_where_cdt(available_prop_ids, propertynps, prop_mat, use_aggr_for_left_prop=False):
	left_prop_id, right_prop_id = choose_proppair_for_cdt(available_prop_ids, available_prop_ids, propertynps, prop_mat)
	if left_prop_id is None and right_prop_id is None:
		return None
	left_prop = copy.deepcopy(propertynps[left_prop_id])
	right_prop = copy.deepcopy(propertynps[right_prop_id])
	# there must not be aggregators in where conditions
	assert (left_prop.aggr == 0)
	assert (right_prop.aggr == 0)

	if use_aggr_for_left_prop:
		# assign different distributions for aggregators according to the property dtypes
		aggr_id = numpy.random.choice(numpy.arange(len(AGGREGATION_FUNCTIONS)), p=AGGR_DISTS[left_prop.dtype])
		left_prop.set_aggr(aggr_id)
	# does not allow for 'between' comparer for column-column conditions or column-subquery conditions
	cmper_distribution = transform2distribution_proportional(numpy.array(CMP_DISTS[left_prop.dtype][1:]))
	cmper_distribution[6] = 0
	cmper_distribution[7] = 0  # like operator at index 9-1-1=7 can never appear in c-c or c-i conditions
	cmper_distribution = transform2distribution_proportional(cmper_distribution)
	cmper = copy.deepcopy(numpy.random.choice(CMPERS[1:], p=cmper_distribution))
	if cmper.index in [8, 9]:
		rho = random.random()
		if rho < 0.1:
			cmper.set_negative()

	c_english = left_prop.c_english.format('') + cmper.c_english.format(right_prop.c_english.format(''))
	c_chinese = left_prop.c_chinese.format('') + cmper.c_chinese.format(right_prop.c_chinese.format(''))
	cdt = CDT(c_english, c_chinese, None, left_prop, right_prop, cmper)
	return cdt


# sub-queries can be either around properties with foreign key relations or same names, but rarely anything else
def construct_ci_where_cdt(available_prop_ids, typenps, propertynps, type_mat, prop_mat, prop_rels, fk_rels,
						   use_aggr_for_left_prop=False, cursor=None, no_negative=False, verbose=False):
	# only allow those with actual values
	all_prop_ids = []
	for i in range(len(propertynps)):
		if propertynps[i].valid:
			all_prop_ids.append(i)
	assert len(all_prop_ids) > 0
	left_prop_id, right_prop_id = choose_proppair_for_cdt(available_prop_ids, all_prop_ids, propertynps, prop_mat)
	if left_prop_id is None and right_prop_id is None:
		return None
	left_prop = copy.deepcopy(propertynps[left_prop_id])

	# does not allow for 'between' comparer for column-column conditions or column-subquery conditions
	cmper_distribution = transform2distribution_proportional(numpy.array(CMP_DISTS[left_prop.dtype][1:]))
	cmper_distribution[7] = 0  # like operator at index 9-1-1=7 can never appear in c-c or c-i conditions
	cmper = None
	if left_prop.dtype in CALCULABLE_DTYPES:
		cmper = copy.deepcopy(numpy.random.choice(CMPERS[1:], p=cmper_distribution))
	else:
		for item in CMPERS:
			if item.index == 8:  # if the comparer is 'in'
				cmper = copy.deepcopy(item)
	assert (cmper is not None)
	if cmper.index in [8, 9]:
		rho = random.random()
		if rho < 0.1 and not no_negative:
			cmper.set_negative()
	# the aggregators can only be max, min or avg (means can't have aggregator for non-number dtypes)
	if cmper.index != 8:  # if comparer is not 'in'
		use_aggr = numpy.random.choice(numpy.arange(len(AGGREGATION_FUNCTIONS)), p=AGGR_SUBQDIST)
	else:
		use_aggr = 0

	prop4right_subq = copy.deepcopy(propertynps[right_prop_id])
	prop4right_subq.set_aggr(use_aggr)

	right_subq_np, right_subq_qrynp = scratch_build(typenps, propertynps, type_mat, prop_mat, prop_rels, fk_rels,
													is_recursive=True,
													specific_props=[prop4right_subq], cursor=cursor,
													require_singlereturn=(cmper.index != 8), print_verbose=verbose)

	if use_aggr_for_left_prop:
		# assign different distributions for aggregators according to the property dtypes
		aggr_id = numpy.random.choice(numpy.arange(len(AGGREGATION_FUNCTIONS)), p=AGGR_DISTS[left_prop.dtype])
		left_prop.set_aggr(aggr_id)

	c_english = left_prop.c_english.format('') + cmper.c_english.format(
		' ( ' + right_subq_qrynp.c_english + ' ) ')
	c_chinese = left_prop.c_chinese.format('') + cmper.c_chinese.format('（' + right_subq_qrynp.c_chinese + '）')
	z = left_prop.z + cmper.z.format(' ( ' + right_subq_qrynp.z + ' ) ')
	cdt = CDT(c_english, c_chinese, z, left_prop, right_subq_qrynp, cmper)
	return cdt


# convert a query in json format into a NP type object
# pid needs to +1 because 0 is occupied by * in SPIDER format
def np_from_entry(entry_sql, typenps, propertynps, fk_rels, finalize=False):
	table_ids = []
	for item in entry_sql['from']['table_units']:
		if item[0] == 'sql':
			return "'from' not a table!"
		elif item[0] == 'table_unit':
			table_ids.append(item[1])
		else:
			raise AssertionError

	join_cdts = []
	for cond in entry_sql['from']['conds']:
		if isinstance(cond, str):
			assert cond == 'and'
			continue
		assert cond[1] == 2
		assert cond[2][0] == 0 and cond[2][1][0] == 0 and cond[2][1][2] is False and cond[2][2] is None
		assert cond[3][0] == 0 and cond[3][2] is False

		pid1 = cond[2][1][1]-1
		pid2 = cond[3][1]-1
		assert pid1 >= 0 and pid2 >= 0
		prop1 = copy.deepcopy(propertynps[pid1])
		prop2 = copy.deepcopy(propertynps[pid2])

		cmper = copy.deepcopy(CMPERS[1])  # always use equality comparer
		c_english = propertynps[pid1].c_english.format(
			typenps[prop1.table_id].c_english + '\'s ') + cmper.c_english.format(
			propertynps[pid2].c_english.format(typenps[prop2.table_id].c_english + '\'s '))
		c_chinese = propertynps[pid1].c_chinese.format(
			typenps[prop1.table_id].c_chinese + '的') + cmper.c_chinese.format(
			propertynps[pid2].c_chinese.format(typenps[prop2.table_id].c_chinese + '的'))
		z = propertynps[pid1].z + cmper.z.format(propertynps[pid2].z)
		fetched_cdt = CDT(c_english, c_chinese, z, prop1, prop2, cmper)
		join_cdts.append(fetched_cdt)

	tids_is_left = {item: False for item in table_ids}
	tids_is_right = {item: False for item in table_ids}
	for item in join_cdts:
		if item.left.is_fk_left and item.right.is_fk_right:
			tids_is_left[item.left.table_id] = True
			tids_is_right[item.right.table_id] = True
		elif item.left.is_fk_right and item.right.is_fk_left:
			tids_is_left[item.right.table_id] = True
			tids_is_right[item.left.table_id] = True
	main_tid = []
	join_simplifiable = True
	if len(table_ids) > 1:
		for tid in table_ids:
			if tids_is_left[tid] is True and tids_is_right[tid] is False:
				if len(main_tid) > 0:
					print("!")
				main_tid.append(tid)
			# for cases where it's about two instances of the same table
			elif len(tids_is_left) == 1 and len(tids_is_right) == 1:
				main_tid.append(tid)
				join_simplifiable = False
			# if medium tables exist or multiple instances of same table exist, join conditions cannot be simplified
			# because it'll lose the dependency hierarchy
			elif tids_is_left[tid] is True and tids_is_right[tid] is True:
				join_simplifiable = False
	else:
		main_tid = [table_ids[0]]

	# check whether the tables joined together have multiple foreign-key reations, if so, then it's not simplifiable
	if join_simplifiable and len(main_tid) > 0:
		for item in join_cdts:
			tid1 = item.left.table_id
			tid2 = item.right.table_id
			join_cdts_cnt = 0
			for rel in fk_rels:
				if rel.table_ids[0] == tid1 and rel.table_ids[1] == tid2:
					join_cdts_cnt += 1
				elif rel.table_ids[0] == tid2 and rel.table_ids[1] == tid1:
					join_cdts_cnt += 1
			# if there are multiple foreign key relationships between table pairs in this query, then by simplifying
			# canonical utterance of join conditions ambiguity would occur
			if join_cdts_cnt > 1:
				join_simplifiable = False

	queried_props = []
	for item in entry_sql['select'][1]:
		aggr = item[0]
		if item[1][0] != 0:
			return "calculation operator in select!"
		pid = item[1][1][1]-1
		aggr_distinct = item[1][1][2]
		assert item[1][2] is None
		if pid < 0:
			prop = copy.deepcopy(STAR_PROP)
			prop.table_id = table_ids
			assert aggr_distinct is False
		else:
			prop = copy.deepcopy(propertynps[pid])  # !!!
		if aggr == 3:
			prop.set_aggr(3, function=_count_uniqueness_specified, distinct=aggr_distinct)
			if pid < 0:
				# if there are single2multiple foreign key relationships between the multiple tables
				#
				# if there are multiple join-on heads, then it'd be unclear which one is to be counted,
				# in such cases we rollback to using traditional 'number of entries'
				if len(main_tid) == 1:
					type_to_count = typenps[main_tid[0]]
					prop.c_english = ' the number of %s ' % type_to_count.c_english
					prop.c_chinese = '%s的数量' % type_to_count.c_chinese
		else:
			prop.set_aggr(aggr)
		queried_props.append(prop)

	cdts = []
	cdt_linkers = []
	for cond in entry_sql['where']:
		if isinstance(cond, str):
			assert cond == 'or' or cond == 'and'
			cdt_linkers.append(cond)
			continue

		negative = cond[0]
		cmper = None
		cmper_idx = cond[1]
		for item in CMPERS:
			if item.index == cmper_idx:
				cmper = copy.deepcopy(item)
				break
		assert cmper is not None
		if cmper_idx == 9:
			print(str(cond[3]))
			if str(cond[3]).count('%') == 2:
				cmper.set_mode('mid')
			elif str(cond[3])[1] == '%':
				cmper.set_mode('tail')
			elif str(cond[3])[-2] == '%':
				cmper.set_mode('head')
			elif '%' not in str(cond[3]):
				cmper.set_mode('mid')
			else:
				raise AssertionError
		if negative:
			cmper.set_negative()

		# does not support calculation operators
		if cond[2][0] != 0:
			return "calculation operator in 'where'!"

		pid1 = cond[2][1][1]-1
		assert cond[2][1][0] == 0 and cond[2][1][2] is False and cond[2][2] is None
		assert pid1 >= 0
		chosen_prop = copy.deepcopy(propertynps[pid1])
		value = cond[3]

		if isinstance(value, list):
			assert len(value) == 3
			assert cond[4] is None
			pid2 = value[1]-1
			assert pid2 >= 0
			prop2 = copy.deepcopy(propertynps[pid2])
			c_english = chosen_prop.c_english.format('') + cmper.c_english.format(prop2.c_english.format(''))
			c_chinese = chosen_prop.c_chinese.format('') + cmper.c_chinese.format(prop2.c_chinese.format(''))
			cdt = CDT(c_english, c_chinese, None, chosen_prop, prop2, cmper)
			cdts.append(cdt)

		elif isinstance(value, dict):
			if cond[4] is not None:
				return "between operator with sub-query and value!"
			try:
				pack = np_from_entry(value, typenps, propertynps, fk_rels)
			except KeyError as e:
				raise
			if isinstance(pack, str):
				return pack
			else:
				np_2, qrynp_2 = pack

			c_english = chosen_prop.c_english.format('') + cmper.c_english.format(
				' ( ' + qrynp_2.c_english + ' ) ')
			c_chinese = chosen_prop.c_chinese.format('') + cmper.c_chinese.format('（' + qrynp_2.c_chinese + '）')
			z = chosen_prop.z + cmper.z.format(' ( ' + qrynp_2.z + ' ) ')
			cdt = CDT(c_english, c_chinese, z, chosen_prop, qrynp_2, cmper)
			cdts.append(cdt)
		else:
			try:
				value = VALUENP(str(value), str(value), str(value), chosen_prop.dtype)
				value_z = '#' + value.z + '#'
				value_str = value.z
			except Exception as e:
				raise
			if cond[4] is not None:
				try:
					value_2 = cond[4]
					value_2 = VALUENP(str(value_2), str(value_2), str(value_2), chosen_prop.dtype)
					value_z_2 = '#' + value_2.z + '#'
					value_str_2 = value_2.z
				except Exception as e:
					raise
				value = [value, value_2]
				value_str = [value_str, value_str_2]
				value_z = [value_z, value_z_2]
			else:
				value = [value]
			c_english = chosen_prop.c_english.format('') + cmper.c_english.format(value_str)
			c_chinese = chosen_prop.c_chinese.format('') + cmper.c_chinese.format(value_str)
			z = chosen_prop.z + ' ' + cmper.z.format(value_z)
			cdt = CDT(c_english, c_chinese, z, chosen_prop, value, cmper)
			cdts.append(cdt)
	if len(cdts) > 0:
		assert len(cdts) == (len(cdt_linkers) + 1)

	group_props = []
	for item in entry_sql['groupBy']:
		assert item[0] == 0 and item[2] is False
		gb_pid = item[1]-1
		assert gb_pid >= 0
		gb_prop = copy.deepcopy(propertynps[gb_pid])
		group_props.append(gb_prop)

	having_cdts = []
	if len(entry_sql['having']) > 1:
		return "more than one having condition!"
	for cond in entry_sql['having']:
		assert cond[0] is False
		cmper = None
		cmper_idx = cond[1]
		for item in CMPERS:
			if item.index == cmper_idx:
				cmper = copy.deepcopy(item)
		assert cmper is not None

		# does not support calculation operators
		if cond[2][0] != 0:
			return "calculation operator in 'having' condition!"

		pid1 = cond[2][1][1] - 1
		aggr = cond[2][1][0]
		aggr_distinct = cond[2][1][2]
		if pid1 < 0:
			chosen_prop = copy.deepcopy(STAR_PROP)
			chosen_prop.table_id = table_ids
			assert aggr_distinct is False
		else:
			chosen_prop = copy.deepcopy(propertynps[pid1])
		if aggr == 3:
			chosen_prop.set_aggr(3, function=_count_uniqueness_specified, distinct=aggr_distinct)
			if pid1 < 0:
				# if there are single2multiple foreign key relationships between the multiple tables, the
				if len(main_tid) == 1:
					type_to_count = typenps[main_tid[0]]
					chosen_prop.c_english = ' the number of %s ' % type_to_count.c_english
					chosen_prop.c_chinese = '%s的数量' % type_to_count.c_chinese
		else:
			chosen_prop.set_aggr(aggr)
		value = cond[3]

		assert not isinstance(value, list)
		if isinstance(value, dict):
			assert cond[4] is None
			try:
				np_2, qrynp_2 = np_from_entry(value, typenps, propertynps, fk_rels)
			except KeyError as e:
				raise
			c_english = chosen_prop.c_english.format('') + cmper.c_english.format(
				' ( ' + qrynp_2.c_english + ' ) ')
			c_chinese = chosen_prop.c_chinese.format('') + cmper.c_chinese.format('（' + qrynp_2.c_chinese + '）')
			z = chosen_prop.z + cmper.z.format(' ( ' + qrynp_2.z + ' ) ')
			cdt = CDT(c_english, c_chinese, z, chosen_prop, qrynp_2, cmper)
			having_cdts.append(cdt)
		else:
			try:
				value = VALUENP(str(value), str(value), str(value), chosen_prop.dtype)
				value_z = '#' + value.z + '#'
				value_str = value.z
			except Exception as e:
				raise
			if cond[4] is not None:
				try:
					value_2 = cond[4]
					value_2 = VALUENP(str(value_2), str(value_2), str(value_2), chosen_prop.dtype)
					value_z_2 = '#' + value_2.z + '#'
					value_str_2 = value_2.z
				except Exception as e:
					raise
				value = [value, value_2]
				value_str = [value_str, value_str_2]
				value_z = [value_z, value_z_2]
			else:
				value = [value]
			c_english = chosen_prop.c_english.format('') + cmper.c_english.format(value_str)
			c_chinese = chosen_prop.c_chinese.format('') + cmper.c_chinese.format(value_str)
			z = chosen_prop.z + ' ' + cmper.z.format(value_z)
			cdt = CDT(c_english, c_chinese, z, chosen_prop, value, cmper)
			having_cdts.append(cdt)

	orderby_props = []
	orderby_order = None
	if len(entry_sql['orderBy']) > 0:
		orderby_order = entry_sql['orderBy'][0]
		for item in entry_sql['orderBy'][1]:
			if item[0] != 0:
				return "calculation in 'orderBy' condition!"
			if item[1][2] is not False:
				pass
			aggr = item[1][0]
			ob_pid = item[1][1]-1
			if ob_pid < 0:
				ob_prop = copy.deepcopy(STAR_PROP)
				ob_prop.table_id = table_ids
			else:
				ob_prop = copy.deepcopy(propertynps[ob_pid])
			if aggr == 3:
				ob_prop.set_aggr(3, function=_count_uniqueness_specified, distinct=False)
				if ob_pid < 0:
					# if there are single2multiple foreign key relationships between the multiple tables, the
					if len(main_tid) == 1:
						type_to_count = typenps[main_tid[0]]
						ob_prop.c_english = ' the number of %s ' % type_to_count.c_english
						ob_prop.c_chinese = '%s的数量' % type_to_count.c_chinese
			else:
				ob_prop.set_aggr(aggr)
			orderby_props.append(ob_prop)
	else:
		orderby_props = None

	limit = entry_sql['limit']
	has_union = False
	has_except = False
	has_intersect = False
	np_2 = None
	qrynp_2 = None

	if entry_sql['intersect'] is not None:
		assert entry_sql['union'] is None and entry_sql['except'] is None
		try:
			pack = np_from_entry(entry_sql['intersect'], typenps, propertynps, fk_rels)
			has_intersect = True
		except KeyError as e:
			raise
		if isinstance(pack, str):
			return pack
		else:
			np_2, qrynp_2 = pack

	if entry_sql['union'] is not None:
		assert entry_sql['intersect'] is None and entry_sql['except'] is None
		try:
			pack = np_from_entry(entry_sql['union'], typenps, propertynps, fk_rels)
			has_union = True
		except KeyError as e:
			raise
		if isinstance(pack, str):
			return pack
		else:
			np_2, qrynp_2 = pack

	if entry_sql['except'] is not None:
		assert entry_sql['union'] is None and entry_sql['intersect'] is None
		try:
			pack = np_from_entry(entry_sql['except'], typenps, propertynps, fk_rels)
			has_except = True
		except KeyError as e:
			raise
		if isinstance(pack, str):
			return pack
		else:
			np_2, qrynp_2 = pack

	distinct = entry_sql['select'][0]

	if not join_simplifiable:
		main_tid = []
	final_np = NP(queried_props=queried_props, table_ids=table_ids, join_cdts=join_cdts, cdts=cdts,
				  cdt_linkers=cdt_linkers, group_props=group_props, having_cdts=having_cdts, orderby_props=orderby_props,
				  orderby_order=orderby_order, limit=limit, np_2=np_2, qrynp_2=qrynp_2, has_union=has_union,
				  has_except=has_except, has_intersect=has_intersect, distinct=distinct, main_tid=main_tid)
	final_qrynp = QRYNP(final_np, typenps=typenps, propertynps=propertynps, finalize_sequence=finalize)
	return final_np, final_qrynp


# type_mat的连接是单向的；prop_mat的连接是双向的，且矩阵是对称的；这里的property不包括*
# is_recursive is False & specific_props is not None	->		是一个union / except / intersect 子语句
def scratch_build(typenps, propertynps, type_mat, prop_mat, prop_rels, fk_rels, is_recursive=False, specific_props=None,
				  print_verbose=False, finalize_sequence=False, cursor=None, require_singlereturn=False):
	res_is_single = False  # initialize result status as not single
	# set random seeds
	random.seed()
	numpy.random.seed()
	total_valid_tables = len(typenps)
	if print_verbose:
		print("-----------")
		print("scratch_build_began")
	# about tables to join
	join_cdts = []
	prior_importance = calc_importance(type_mat, [])
	for tid in range(len(typenps)):
		if typenps[tid].valid is False:
			prior_importance[tid] = -10000
			total_valid_tables -= 1
	num_tables = numpy.random.choice([1, 2, 3, 4], p=num_tables_distribution[is_recursive])
	num_tables = min(num_tables, total_valid_tables)
	# initialize table_distribution with prior distribution; after that, update the distribution
	# according to settled tables.
	table_distribution = transform2distribution(numpy.array(prior_importance, dtype=numpy.float))
	if specific_props is None:
		table_ids = [numpy.random.choice(numpy.arange(len(typenps)), p=table_distribution)]
	else:
		table_ids = []
		contain_star_flag = False
		for prop in specific_props:
			# if the property is a '*' property
			if isinstance(prop.table_id, list):
				contain_star_flag = True
				for item in prop.table_id:
					if item not in table_ids:
						table_ids.append(item)
			else:
				if prop.table_id not in table_ids:
					table_ids.append(prop.table_id)
		# if queried properties contains '*', adding more tables would result in unaligned column numbers between two
		# sides of 'union/except/intersect'
		if contain_star_flag:
			num_tables = len(table_ids)
	initial_length = len(table_ids)
	for _ in range(initial_length, num_tables):
		allow_pseudo_rels = False
		table_importance = calc_importance(type_mat, table_ids, prior_importance)
		for tid in range(len(typenps)):
			if typenps[tid].valid is False:
				prior_importance[tid] = -10000
		table_distribution = transform2distribution(table_importance)
		for idx in range(len(typenps)):
			if idx in table_ids:
				table_distribution[idx] = -10000
		all_zero_flag = True
		for item in table_distribution:
			if item > 0:
				all_zero_flag = False
		if all_zero_flag:
			rho = random.random()
			if rho < 0.5:
				num_tables = _
				table_ids = table_ids[:_]
				break
			else:
				table_distribution = [1] * len(table_distribution)
				allow_pseudo_rels = True
		table_distribution = transform2distribution_proportional(table_distribution)
		new_tid = numpy.random.choice(numpy.arange(len(typenps)), p=table_distribution)
		cur_join_cdt = fetch_join_rel(new_tid, table_ids, prop_rels, typenps, propertynps, allow_pseudo_rels)
		join_cdts.append(cur_join_cdt)
		table_ids.append(new_tid)

	# here it should be guaranteed that each added table should have at least some
	# connected columns with some existing tables (either foreign key or same name)
	table_actived = {}
	for tid in table_ids:
		table_actived[tid] = False

	tids_is_left = {item: False for item in table_ids}
	tids_is_right = {item: False for item in table_ids}
	for item in join_cdts:
		if item.left.is_fk_left and item.right.is_fk_right:
			tids_is_left[item.left.table_id] = True
			tids_is_right[item.right.table_id] = True
		elif item.left.is_fk_right and item.right.is_fk_left:
			tids_is_left[item.right.table_id] = True
			tids_is_right[item.left.table_id] = True
	main_tid = []
	join_simplifiable = True
	if len(table_ids) > 1:
		for tid in table_ids:
			if tids_is_left[tid] is True and tids_is_right[tid] is False:
				if len(main_tid) > 0:
					print("!!")
				main_tid.append(tid)
			# for cases where it's about two instances of the same table
			elif len(tids_is_left) == 1 and len(tids_is_right) == 1:
				main_tid.append(tid)
				join_simplifiable = False
			# if medium tables exist or multiple instances of same table exist, join conditions cannot be simplified
			# because it'll lose the dependency hierarchy
			elif tids_is_left[tid] is True and tids_is_right[tid] is True:
				join_simplifiable = False
	else:
		main_tid = [table_ids[0]]

	if join_simplifiable and len(main_tid) > 0:
		for item in join_cdts:
			tid1 = item.left.table_id
			tid2 = item.right.table_id
			join_cdts_cnt = 0
			for rel in fk_rels:
				if rel.table_ids[0] == tid1 and rel.table_ids[1] == tid2:
					join_cdts_cnt += 1
				elif rel.table_ids[0] == tid2 and rel.table_ids[1] == tid1:
					join_cdts_cnt += 1
			# if there are multiple foreign key relationships between table pairs in this query, then by simplifying
			# canonical utterance of join conditions ambiguity would occur
			if join_cdts_cnt > 1:
				join_simplifiable = False

	if print_verbose:
		print("tables set~")

	available_prop_ids = []
	for tabid in table_ids:
		if tabid is None:
			raise AssertionError
		# only add those with actual values instead of None
		for prop_id in typenps[tabid].properties:
			if propertynps[prop_id].valid:
				available_prop_ids.append(prop_id)
	if len(available_prop_ids) == 0:
		raise AssertionError

	# about where conditions
	num_wheres = numpy.random.choice([0, 1, 2, 3, 4], p=num_wheres_distribution[is_recursive])
	where_cdts = []
	where_linkers = []
	where_has_same_entity = False
	# c-v where conditions and c-i where conditions have a proportion of 7 to 1
	where_cnt = 0

	cur_np = NP(prev_np=None, queried_props=[copy.deepcopy(STAR_PROP)], table_ids=table_ids, join_cdts=join_cdts,
				orderby_props=[copy.deepcopy(propertynps[available_prop_ids[0]])], orderby_order='asc',
				limit=MAX_RETURN_ENTRIES)
	cur_qrynp = QRYNP(cur_np, typenps=typenps, propertynps=propertynps)
	prev_res = cursor.execute(cur_qrynp.z).fetchall()
	ci_occured_flag = False
	_turn_cnter = 0
	while where_cnt < num_wheres:
		cur_is_ci_flag = False
		_turn_cnter += 1
		if _turn_cnter > 15 or res_is_single:
			num_wheres = where_cnt
			break
		rho = random.random()
		# if already is a recursed function instance, don't dive deeper again
		if rho < 0.2 and is_recursive is False and not ci_occured_flag:
			current_where_cdt = construct_ci_where_cdt(available_prop_ids, typenps, propertynps, type_mat, prop_mat,
													   prop_rels, fk_rels, cursor=cursor, verbose=print_verbose)
			if current_where_cdt is not None:
				ci_occured_flag = True
				cur_is_ci_flag = True
			where_has_same_entity = False
		elif rho < 0.3 and where_cnt == num_wheres - 2:
			current_where_cdt = construct_cv_where_cdt(available_prop_ids, propertynps)
			where_cdts.append(current_where_cdt)
			where_linkers.append('or')
			where_has_same_entity = True
			current_where_cdt = construct_cv_where_cdt(available_prop_ids, propertynps,
													   assigned_prop=current_where_cdt.left)
		elif rho < 0.95:
			current_where_cdt = construct_cv_where_cdt(available_prop_ids, propertynps)
			where_has_same_entity = False
		else:
			current_where_cdt = construct_cc_where_cdt(available_prop_ids, propertynps, prop_mat)
			where_has_same_entity = False
		# there is guaranteed to have a c-v condition
		if current_where_cdt is None:
			current_where_cdt = construct_cv_where_cdt(available_prop_ids, propertynps)
			where_has_same_entity = False
		where_cdts.append(current_where_cdt)

		# set corresponding table_activated to True
		table_actived[current_where_cdt.left.table_id] = True
		if isinstance(current_where_cdt.right, PROPERTYNP):
			table_actived[current_where_cdt.right.table_id] = True

		if where_cnt > 0:
			# choose a linker between 'and' & 'or', with proportion of 511:199 according to SPIDER train set
			rho = random.random()
			if rho < 0.18:
				where_linkers.append('or')
			else:
				where_linkers.append('and')

		# $execution-guidance
		cur_np = NP(prev_np=None, queried_props=[copy.deepcopy(STAR_PROP)], table_ids=table_ids, join_cdts=join_cdts,
					cdts=where_cdts, cdt_linkers=where_linkers,
					orderby_props=[copy.deepcopy(propertynps[available_prop_ids[0]])],
					orderby_order='asc', limit=MAX_RETURN_ENTRIES)
		cur_qrynp = QRYNP(cur_np, typenps=typenps, propertynps=propertynps)

		res = cursor.execute(cur_qrynp.z).fetchall()
		if len(res) == 1:
			res_is_single = True
		elif len(res) == 0 or len(res) == len(prev_res):
			if where_cnt > 0:
				num_wheres -= 1
			if where_has_same_entity:
				where_cdts = where_cdts[:-2]
			else:
				where_cdts = where_cdts[:-1]
			if where_cnt > 0:
				where_linkers = where_linkers[:-1]
			if where_has_same_entity:
				where_linkers = where_linkers[:-1]
			if cur_is_ci_flag:
				ci_occured_flag = False
			continue
		# execution-guidance$
		if where_has_same_entity:
			where_cnt += 2
		else:
			where_cnt += 1
		prev_res = res

	if print_verbose:
		print("where set")
	available_prop_ids_after_where = copy.copy(available_prop_ids)

	# about group-by and having
	having_cdts = []
	rho = random.random()
	# these probabilities come from SPIDER dataset train set statistics
	if rho < 0.08:
		num_groupbys = 2
	elif rho < 1:
		num_groupbys = 1
	else:
		num_groupbys = 0

	groupable_prop_ids = []
	for prop_id in available_prop_ids:
		if propertynps[prop_id].is_unique is False:
			groupable_prop_ids.append(prop_id)
		else:
			rho = random.random()
			if rho < 0.4:
				groupable_prop_ids.append(prop_id)

	for cond in where_cdts:
		# if the left-side column in this condition is involved in a '=' condition (the aggr for left-side column in
		# where conditions must be empty)
		if cond.cmper.index == 2 and cond.cmper.negative is False:
			# exclude it from groupable_prop_ids since those columns would have only one value left and there'd be no
			# need to group-by
			if cond.left.overall_idx in groupable_prop_ids:
				groupable_prop_ids.remove(cond.left.overall_idx)
			# if the equality condition is imposed on a unique column, then it means there'd be only one value entry
			# in the result altogether, therefore there'd be no need for group-bys or order-bys
			if cond.left.is_unique:
				num_groupbys = 0

	num_groupbys = min(num_groupbys, len(groupable_prop_ids), len(available_prop_ids) - 1)

	# if query result contains only one entry, allow no group-by
	if res_is_single:
		num_groupbys = 0

	if num_groupbys > 0:
		groupby_prop_ids, available_after_group_by_prop_ids = choose_groupby_prop(groupable_prop_ids, propertynps,
																				  num_groupbys)
		groupby_prop_ids = groupby_prop_ids.tolist()
		groupby_props = []
		discarded_groupby_propids = []
		for i, idx in enumerate(groupby_prop_ids):
			# $execution-guidance
			_prop = copy.deepcopy(STAR_PROP)
			_prop.set_aggr(3)
			cur_np = NP(prev_np=None, queried_props=[_prop], table_ids=table_ids, join_cdts=join_cdts,
						cdts=where_cdts, cdt_linkers=where_linkers,
						group_props=groupby_props + [copy.deepcopy(propertynps[idx])], having_cdts=None)
			cur_qrynp = QRYNP(cur_np, typenps=typenps, propertynps=propertynps)
			res = cursor.execute(cur_qrynp.z).fetchall()
			all_one = True
			for item in res:
				if item[0] != 1:
					all_one = False
					break
			# if there are no more than 1 value after 'group by' or each group have only one value
			if len(res) <= 1 or all_one:
				discarded_groupby_propids.append(idx)
			else:
				# execution-guidance$
				groupby_props.append(copy.deepcopy(propertynps[idx]))
		for pid in discarded_groupby_propids:
			groupby_prop_ids.remove(pid)
		available_after_group_by_prop_ids = find_available_propids_after_groupby(propertynps, groupby_prop_ids)
		num_groupbys = len(groupby_prop_ids)

		if num_groupbys == 0:
			groupby_prop_ids = None
			available_after_group_by_prop_ids = None
		else:
			# set corresponding table_activated to True
			for prop in groupby_props:
				table_actived[prop.table_id] = True

			# from statistics we also know that more than one 'having' conditions need not be considered
			rho = random.random()
			having_usable_prop_ids = []
			for idx in available_prop_ids:
				if idx not in groupby_prop_ids:
					having_usable_prop_ids.append(idx)
			if rho < 0.03:

				cur_having_cdt = construct_ci_where_cdt(having_usable_prop_ids, typenps, propertynps, type_mat, prop_mat,
													prop_rels, fk_rels, True, cursor=cursor, no_negative=True,
													verbose=print_verbose)
				if cur_having_cdt is not None:
					having_cdts.append(cur_having_cdt)
			elif rho < 0.15:
				cur_having_cdt = construct_cv_where_cdt(having_usable_prop_ids, propertynps, True, no_negative=True)
				if cur_having_cdt is not None:
					having_cdts.append(cur_having_cdt)
			elif rho < 0.4:
				_star = copy.deepcopy(STAR_PROP)
				_star.set_aggr(3)

				# if there are single2multiple foreign key relationships between the multiple tables, the
				if len(main_tid) == 1:
					type_to_count = typenps[main_tid[0]]
					_star.c_english = ' the number of %s ' % type_to_count.c_english
					_star.c_chinese = '%s的数量' % type_to_count.c_chinese
				having_cdts.append(construct_cv_where_cdt([], propertynps, True, _star, no_negative=True))
			assert not None in having_cdts
			# if all groups or no groups are excluded after this 'having_cdts', then don't add this 'having_cdts'
			_prop = copy.deepcopy(STAR_PROP)
			_prop.set_aggr(3)
			prev_np = NP(prev_np=None, queried_props=[_prop], table_ids=table_ids, join_cdts=join_cdts,
						 cdts=where_cdts, cdt_linkers=where_linkers,
						 group_props=groupby_props)
			prev_qrynp = QRYNP(prev_np, typenps=typenps, propertynps=propertynps)
			prev_res = cursor.execute(prev_qrynp.z).fetchall()
			cur_np = NP(prev_np=None, queried_props=[_prop], table_ids=table_ids, join_cdts=join_cdts,
						cdts=where_cdts, cdt_linkers=where_linkers,
						group_props=groupby_props, having_cdts=having_cdts)
			cur_qrynp = QRYNP(cur_np, typenps=typenps, propertynps=propertynps)
			res = cursor.execute(cur_qrynp.z).fetchall()
			if len(res) == 0 or len(res) == len(prev_res):
				if len(res) == len(prev_res):
					pass
				else:
					having_cdts = []

			# set corresponding table_activated to True
			if having_cdts is not None and len(having_cdts) > 0:
				assert len(having_cdts) == 1
				table_actived[having_cdts[0].left.table_id] = True
	else:
		groupby_prop_ids = None
		groupby_props = []
		available_after_group_by_prop_ids = None

	if print_verbose:
		print("group-by set~")

	rho = random.random()
	if specific_props is not None:
		props2query = []
		for prop in specific_props:
			assert (isinstance(prop, PROPERTYNP))
			prop = copy.deepcopy(prop)
			if groupby_prop_ids is not None and prop.overall_idx not in available_after_group_by_prop_ids and prop.dtype != 'star':
				# if the specified prop is already aggregated, just keep that aggregator
				if prop.aggr != 0:
					pass
				elif prop.dtype in SUBTRACTABLE_DTYPES:
					probs = copy.deepcopy(AGGR_DISTS[prop.dtype])
					probs[3] = 0
					probs = transform2distribution_proportional(probs)
					aggr_id = numpy.random.choice(numpy.arange(len(AGGREGATION_FUNCTIONS)), p=probs)
					prop.set_aggr(aggr_id)
				else:
					# if columns which must be aggregated due to group-by cannot be aggregated due to its being a
					# specified prop, then delete the group-by clause
					groupby_prop_ids = None
					groupby_props = []
					available_after_group_by_prop_ids = None
					having_cdts = None
			props2query.append(prop)
	elif rho < 0.05:
		props2query = [copy.deepcopy(STAR_PROP)]
		props2query[0].table_id = table_ids
	else:
		num_props2query = numpy.random.choice([0, 1, 2, 3, 4], p=num_selected_distribution)
		num_props2query = min(num_props2query, len(available_prop_ids))
		query_probabilities = calc_forquery_distribution(available_prop_ids, groupby_prop_ids, propertynps)
		propids2query = []
		if groupby_prop_ids is not None and len(groupby_prop_ids) > 0:
			rho = random.random()
			if rho < 0.8:
				propids2query += groupby_prop_ids
				for pid in groupby_prop_ids:
					query_probabilities[pid] = 0
					num_props2query -= 1
		num_props2query = max(num_props2query, 0)

		# if a column's value is already settled by an equality 'where' condition, then there is no need to query it
		# anymore, since it's something we already know before our query.
		for cdt in where_cdts:
			if cdt.cmper.index == 2:
				query_probabilities[cdt.left.overall_idx] = 0.000001
		query_probabilities = transform2distribution_proportional(query_probabilities)

		propids2query += numpy.random.choice(numpy.arange(len(propertynps)), num_props2query, replace=False,
											 p=query_probabilities).tolist()

		for i in propids2query:
			assert (i in available_prop_ids)
			# set corresponding table_activated to True
			table_actived[propertynps[i].table_id] = True

		# if no props are specified, '*' is not the only queried prop, and there are tables not attended to up till
		# now, add a random column from that table to queried-props so that it can get involved
		for key in table_actived:
			if table_actived[key] is False:
				additional_props_to_select = typenps[key].properties
				propids2query.append(numpy.random.choice(additional_props_to_select))
		num_props2query = len(propids2query)
		props2query = []
		for idx in propids2query:
			chosen_prop = copy.deepcopy(propertynps[idx])
			rho = random.random()
			if groupby_prop_ids is not None and idx not in available_after_group_by_prop_ids:
				aggr_id = numpy.random.choice(numpy.arange(len(AGGREGATION_FUNCTIONS)), p=AGGR_DISTS[chosen_prop.dtype])
				chosen_prop.set_aggr(aggr_id)
			elif groupby_prop_ids is None and rho < 0.4:

				# if there is only one value for this column in query, then we only by probability of 0.05 use 'count()'
				cur_np = NP(prev_np=None, queried_props=[copy.deepcopy(chosen_prop)], table_ids=table_ids,
							join_cdts=join_cdts,
							cdts=where_cdts, cdt_linkers=where_linkers,
							group_props=groupby_props, having_cdts=having_cdts,
							orderby_props=[copy.deepcopy(propertynps[available_prop_ids[0]])],
							orderby_order='asc', limit=MAX_RETURN_ENTRIES)
				cur_qrynp = QRYNP(cur_np, typenps=typenps, propertynps=propertynps)
				res = cursor.execute(cur_qrynp.z).fetchall()
				if len(res) > 1 or rho < 0.05:
					aggr_id = numpy.random.choice(numpy.arange(len(AGGREGATION_FUNCTIONS)),
												  p=AGGR_DISTS[chosen_prop.dtype])
					chosen_prop.set_aggr(aggr_id)
			props2query.append(chosen_prop)

		if rho < 0.15:
			props2query.pop()
			_star = copy.deepcopy(STAR_PROP)
			_star.table_id = table_ids
			rho = random.random()
			# for queries with group-by, count aggregator has to be added
			if rho < 0.9 or groupby_prop_ids is not None:
				_star.set_aggr(3)
				# if there are single2multiple foreign key relationships between the multiple tables, the
				if len(main_tid) == 1:
					type_to_count = typenps[main_tid[0]]
					_star.c_english = ' the number of %s ' % type_to_count.c_english
					_star.c_chinese = '%s的数量' % type_to_count.c_chinese
			props2query.append(_star)

	if print_verbose:
		print("properties to query set")

	if groupby_prop_ids is None:
		props2query_covered_by_groupbyids = False
	else:
		props2query_covered_by_groupbyids = True
		for prop in props2query:
			if prop.overall_idx not in groupby_prop_ids:
				props2query_covered_by_groupbyids = False

	# check if all selected values are aggregated, if so, allow no more 'orderby count(*)'
	all_aggregated = True
	for prop in props2query:
		if prop.aggr == 0:
			all_aggregated = False
			break

	# about order by; no order-by for recursed sub-queries
	rho = abs(random.gauss(0, 1))

	# we don't order-by when there is only one entry returned
	cur_np = NP(prev_np=None, queried_props=props2query, table_ids=table_ids, join_cdts=join_cdts,
				cdts=where_cdts, cdt_linkers=where_linkers,
				group_props=groupby_props, having_cdts=having_cdts,
				orderby_props=[copy.deepcopy(propertynps[available_prop_ids[0]])],
				orderby_order='asc', limit=MAX_RETURN_ENTRIES)
	cur_qrynp = QRYNP(cur_np, typenps=typenps, propertynps=propertynps)
	res = cursor.execute(cur_qrynp.z).fetchall()
	if len(res) <= 1:
		num_orderbys = 0
	elif is_recursive is False and specific_props is not None:
		num_orderbys = 0
	elif require_singlereturn and props2query[0].aggr == 0:
		assert is_recursive
		num_orderbys = 1
	elif is_recursive:
		num_orderbys = 0
	elif rho > 3.4:
		num_orderbys = int(rho)  # allow for a tiny possibility of having more than 3 orderby properties
	elif rho > 2.6:
		num_orderbys = 2
	elif rho > 0.8:
		num_orderbys = 1
	else:
		num_orderbys = 0

	need_orderby_covered = False
	# if groupBy is not used by 'having', and xxx is covered by groupBy columns, then use an grouped-columns related
	# orderBy to use it
	if having_cdts is None or len(having_cdts) == 0:
		if num_groupbys > 0 and props2query_covered_by_groupbyids:
			if is_recursive is True or specific_props is None:
				num_orderbys = max(1, num_orderbys)
				need_orderby_covered = True

	orderby_available_props = []

	if specific_props is None or is_recursive:
		for prop_id in available_prop_ids:
			prop = copy.deepcopy(propertynps[prop_id])
			if prop.dtype == 'star':
				continue
			# if it's not a query with group-by and this column queried is with aggregator, this column is excluded
			# from consideration of order-by since there can only be one value for this column queried
			veto = False
			for cp in props2query:
				if cp.overall_idx == prop_id:
					if num_groupbys == 0 and cp.aggr != 0:
						veto = True
					else:
						prop = copy.deepcopy(cp)
					break
			if veto:
				continue
			if prop.dtype in ORDERBY_DTYPES:
				orderby_available_props.append(prop)
	else:
		for prop in props2query:
			if prop.dtype == 'star':
				continue
			if num_groupbys == 0 and prop.aggr != 0:
				continue
			if prop.dtype in ORDERBY_DTYPES:
				orderby_available_props.append(copy.deepcopy(prop))

	# if all returned entries are the same, don't order by this column
	cur_np = NP(prev_np=None, queried_props=[copy.deepcopy(STAR_PROP)], table_ids=table_ids, join_cdts=join_cdts,
				cdts=where_cdts, cdt_linkers=where_linkers, group_props=groupby_props,
				having_cdts=having_cdts, orderby_props=[copy.deepcopy(propertynps[available_prop_ids[0]])],
				orderby_order='asc', limit=MAX_RETURN_ENTRIES)
	cur_qrynp = QRYNP(cur_np, typenps=typenps, propertynps=propertynps, finalize_sequence=finalize_sequence)
	res = cursor.execute(cur_qrynp.z).fetchall()
	if len(res) == 1000:
		print("!!!!!")
	headers = [tup[0] for tup in cursor.description]
	cur_tid_index = 0
	changed_table = False
	h_idx = 0
	while h_idx < len(headers):
		h = headers[h_idx]
		found = False
		for p in propertynps:
			if p.z.format('').strip('[').strip(']') == h and p.table_id == table_ids[cur_tid_index]:
				found = True
				pid = p.overall_idx
				if pid not in available_prop_ids:
					break
				vals = []
				for ent in res:
					assert len(ent) == len(headers)
					vals.append(ent[h_idx:h_idx + 1])
				if all_the_same(vals):
					available_prop_ids.remove(pid)
				break
		if found:
			h_idx += 1
			changed_table = False
		else:
			assert not changed_table
			cur_tid_index += 1
			changed_table = True

	if print_verbose:
		print("columns available for order-by picked!")

	for cond in where_cdts:
		# if the left-side column in this condition is involved in a '=' condition (the aggr for left-side column in
		# where conditions must be empty)
		if cond.cmper.index == 2 and cond.cmper.negative is False:
			# exclude it from groupable_prop_ids since those columns would have only one value left and there'd be no
			# need to group-by
			for prop in orderby_available_props:
				if cond.left.overall_idx == prop.overall_idx:
					orderby_available_props.remove(prop)
			# if the equality condition is imposed on a unique column, then it means there'd be only one value entry
			# in the result altogether, therefore there'd be no need for group-bys or order-bys
			if cond.left.is_unique:
				num_orderbys = 0

	num_orderbys = min(num_orderbys, len(orderby_available_props))

	limit = None
	orderby_order = None
	orderby_props = None
	# no order-by for 'in' sub-queries
	if num_orderbys > 0:
		# order-by properties are barely relevant to group-by columns, on the other hand,
		# they are to around 25% relevant to selected columns

		# here larger probability of being picked to order-by is assigned to those 'select'ed in query
		ob_probs = [1] * len(orderby_available_props)
		for i, p1 in enumerate(orderby_available_props):
			for p2 in props2query:
				if p1.overall_idx == p2.overall_idx:
					ob_probs[i] += 1.7
			if p1.dtype == 'id':
				ob_probs[i] *= 0.2
			elif p1.dtype == 'int':
				ob_probs[i] *= 1.5
		ob_probs = transform2distribution_proportional(ob_probs)
		orderby_props_raw = numpy.random.choice(orderby_available_props, num_orderbys, p=ob_probs, replace=False)

		orderby_props = []
		if need_orderby_covered:
			orderby_cover_satisfied = False
		else:
			orderby_cover_satisfied = None
		for prop in orderby_props_raw:
			prop = copy.deepcopy(prop)
			# if group-by involved queries have queried props, their prop.aggr != 0, hence those props should not be
			# added more aggrs
			if groupby_prop_ids is not None and prop.overall_idx not in available_after_group_by_prop_ids and prop.aggr == 0:
				aggr_id = numpy.random.choice(numpy.arange(len(AGGREGATION_FUNCTIONS)), p=AGGR_DISTS[prop.dtype])
				prop.set_aggr(aggr_id)
				# if there are orderby columns not in group-by columns, then no extra columns to order by is needed
				orderby_cover_satisfied = True
			orderby_props.append(prop)

		# with a 0.3 probability or is is needed, add a STAR property as orderby-property in order to
		rho = random.random()
		if (rho < 0.85 and num_groupbys > 0) or (
				rho < 0.85 and orderby_cover_satisfied) or orderby_cover_satisfied == False:
			# if it is a union/except/intersect clause, allow only queried props to be ordered, care about usefulness
			# of group-by clauses no more here
			if specific_props is None and not all_aggregated:
				orderby_props.pop()
				_star = copy.deepcopy(STAR_PROP)
				_star.set_aggr(3)
				# if there are single2multiple foreign key relationships between the multiple tables, the
				if len(main_tid) == 1:
					type_to_count = typenps[main_tid[0]]
					_star.c_english = ' the number of %s ' % type_to_count.c_english
					_star.c_chinese = '%s的数量' % type_to_count.c_chinese
				orderby_props.append(_star)

		rho = random.random()
		if rho < 0.368:
			orderby_order = 'asc'
		else:
			orderby_order = 'desc'

		rho = random.random()
		if require_singlereturn:
			limit = 1
		elif rho < 0.676:
			limit_dist = [0.] + [0.05] * 100
			limit_dist[1] = 90
			limit_dist[2] = 2
			limit_dist[3] = 5
			limit_dist[5] = 2
			limit_dist[10] = 1
			for idx in range(5, 101, 5):
				limit_dist[idx] += 0.05
			for idx in range(10, 101, 10):
				limit_dist[idx] += 0.1

			cur_np = NP(prev_np=None, queried_props=props2query, table_ids=table_ids, join_cdts=join_cdts,
						cdts=where_cdts, cdt_linkers=where_linkers, group_props=groupby_props,
						having_cdts=having_cdts)
			cur_qrynp = QRYNP(cur_np, typenps=typenps, propertynps=propertynps, finalize_sequence=finalize_sequence)
			res = cursor.execute(cur_qrynp.z).fetchall()
			len_res_prime = max(len(res), 1)
			limit_dist = limit_dist[:len_res_prime + 1]

			limit = random.choices(range(min(len_res_prime + 1, 101)), weights=limit_dist)[0]
		else:
			limit = None

	if print_verbose:
		print("order-by set~")

	rho = random.random()
	np_2 = None
	qrynp_2 = None
	has_union = None
	has_except = None
	has_intersect = None
	if rho < 0.15 and not is_recursive and specific_props is None and orderby_props is None:
		flag = True
		for prop in props2query:
			if prop.aggr != 0:
				flag = False
				break
		# if no aggregators exist in queried properties
		if flag:
			cur_np = NP(prev_np=None, queried_props=props2query, table_ids=table_ids, join_cdts=join_cdts,
						cdts=where_cdts, cdt_linkers=where_linkers, group_props=groupby_props, having_cdts=having_cdts,
						orderby_props=orderby_props, orderby_order=orderby_order, limit=limit)
			rho = random.random()
			if rho < 0.1:
				# here because the assigned properties might not be compatible with group-by clauses or such, it might not
				# run, if it doesn't run, just discard it
				np_2, qrynp_2 = scratch_build(typenps, propertynps, type_mat, prop_mat, prop_rels, fk_rels, is_recursive=False,
											  specific_props=props2query, cursor=cursor, print_verbose=print_verbose)
			else:
				np_2, qrynp_2 = modify_build(typenps, propertynps, type_mat, prop_mat, prop_rels, fk_rels, cur_np,
											 available_prop_ids_after_where, cursor, print_verbose)
			# choose between using the second query as an 'union' or an 'except'
			rho = random.random()
			if rho < 0.1:
				has_union = True
			elif rho < 0.5:
				has_intersect = True
			else:
				has_except = True

	if np_2 is not None:
		assert qrynp_2 is not None
		cur_np = NP(prev_np=None, queried_props=props2query, table_ids=table_ids, join_cdts=join_cdts,
					cdts=where_cdts, cdt_linkers=where_linkers, group_props=groupby_props, having_cdts=having_cdts,
					orderby_props=orderby_props, orderby_order=orderby_order, limit=limit)
		cur_qrynp = QRYNP(cur_np, typenps=typenps, propertynps=propertynps, finalize_sequence=finalize_sequence)
		res_1 = cursor.execute(cur_qrynp.z).fetchall()
		res_2 = cursor.execute(qrynp_2.z).fetchall()
		if either_contain(res_1, res_2):
			np_2 = None
			qrynp_2 = None
			has_union = False
			has_intersect = False
			has_except = False

	if print_verbose:
		print("post-subquery set~")

	inspect_np = NP(prev_np=None, queried_props=props2query, table_ids=table_ids, join_cdts=join_cdts,
					cdts=where_cdts, cdt_linkers=where_linkers, group_props=groupby_props, having_cdts=having_cdts,
					orderby_props=orderby_props, orderby_order=orderby_order, limit=limit, np_2=np_2, qrynp_2=qrynp_2,
					has_union=has_union, has_except=has_except, has_intersect=has_intersect)
	inspect_qrynp = QRYNP(inspect_np, typenps=typenps, propertynps=propertynps, finalize_sequence=finalize_sequence)
	res = cursor.execute(inspect_qrynp.z).fetchall()
	if len(res) > 200:
		DISTINCT_PROB = 0.2
	elif is_unique(res):
		DISTINCT_PROB = 0.1
	else:
		DISTINCT_PROB = 0.2
	rho = random.random()
	if rho < DISTINCT_PROB:
		distinct = True
	else:
		distinct = False

	if not join_simplifiable:
		main_tid = []
	final_np = NP(prev_np=None, queried_props=props2query, table_ids=table_ids, join_cdts=join_cdts,
				  cdts=where_cdts, cdt_linkers=where_linkers, group_props=groupby_props, having_cdts=having_cdts,
				  orderby_props=orderby_props, orderby_order=orderby_order, limit=limit, np_2=np_2, qrynp_2=qrynp_2,
				  has_union=has_union, has_except=has_except, has_intersect=has_intersect, distinct=distinct,
				  main_tid=main_tid)
	final_qrynp = QRYNP(final_np, typenps=typenps, propertynps=propertynps, finalize_sequence=finalize_sequence)
	if print_verbose:
		print("ALL SET!")
		print("-----------")
	return final_np, final_qrynp


def modify_build(typenps, propertynps, type_mat, prop_mat, prop_rels, fk_rels, last_np, available_prop_ids, cursor,
				 print_verbose):
	rho = random.random()
	final_np = None
	final_qrynp = None

	if (rho < 0.8 and len(last_np.cdts) > 0) or len(last_np.cdts) >= 3:
		to_replace_idx = random.choice(range(len(last_np.cdts)))
		found = False
		while not found:
			rho = random.random()
			# if already is a recursed function instance, don't dive deeper again
			if rho < 0.01:
				current_where_cdt = construct_ci_where_cdt(available_prop_ids, typenps, propertynps, type_mat, prop_mat,
														   prop_rels, fk_rels, cursor=cursor, verbose=print_verbose)
			else:
				rho = random.random()
				if rho < 0.7:
					current_where_cdt = construct_cv_where_cdt(available_prop_ids, propertynps,
															   assigned_prop=last_np.cdts[to_replace_idx].left)
				else:
					current_where_cdt = construct_cv_where_cdt(available_prop_ids, propertynps)
			# there is guaranteed to have a c-v condition
			if current_where_cdt is None:
				current_where_cdt = construct_cv_where_cdt(available_prop_ids, propertynps)

			# $execution-guidance
			cur_np = copy.deepcopy(last_np)
			cur_np.cdts[to_replace_idx] = current_where_cdt
			cur_qrynp = QRYNP(cur_np, typenps=typenps, propertynps=propertynps)
			last_qrynp = QRYNP(cur_np, typenps=typenps, propertynps=propertynps)
			prev_np = copy.deepcopy(cur_np)
			prev_np.cdts = prev_np.cdts[:to_replace_idx] + prev_np.cdts[to_replace_idx + 1:]
			prev_qrynp = QRYNP(prev_np, typenps=typenps, propertynps=propertynps)

			res = cursor.execute(cur_qrynp.z).fetchall()
			prev_res = cursor.execute(prev_qrynp.z).fetchall()
			last_res = cursor.execute(last_qrynp.z).fetchall()

			if len(res) > 0 and len(res) != len(prev_res) and len(res) != len(last_res):
				continue
			else:
				found = True
				final_np = cur_np
				final_qrynp = cur_qrynp
	else:
		found = False
		while not found:
			rho = random.random()
			# if already is a recursed function instance, don't dive deeper again
			if rho < 0.01:
				current_where_cdt = construct_ci_where_cdt(available_prop_ids, typenps, propertynps, type_mat, prop_mat,
														   prop_rels, fk_rels, cursor=cursor, verbose=print_verbose)
			else:
				current_where_cdt = construct_cv_where_cdt(available_prop_ids, propertynps)
			# there is guaranteed to have a c-v condition
			if current_where_cdt is None:
				current_where_cdt = construct_cv_where_cdt(available_prop_ids, propertynps)

			# $execution-guidance
			cur_np = copy.deepcopy(last_np)
			if cur_np.cdts is None:
				cur_np.cdts = []
			if cur_np.cdt_linkers is None:
				cur_np.cdt_linkers = []

			cur_np.cdts.append(current_where_cdt)
			if len(cur_np.cdts) > 0:
				# choose a linker between 'and' & 'or', with proportion of 511:199 according to SPIDER train set
				rho = random.random()
				if rho < 0.18:
					cur_np.cdt_linkers.append('or')
				else:
					cur_np.cdt_linkers.append('and')

			cur_qrynp = QRYNP(cur_np, typenps=typenps, propertynps=propertynps)
			last_qrynp = QRYNP(cur_np, typenps=typenps, propertynps=propertynps)

			res = cursor.execute(cur_qrynp.z).fetchall()
			last_res = cursor.execute(last_qrynp.z).fetchall()

			if len(res) > 0 and len(res) != len(last_res):
				continue
			else:
				found = True
				final_np = cur_np
				final_qrynp = cur_qrynp

	return final_np, final_qrynp
