from scipy.special import softmax
import numpy
from collections import defaultdict
from scipy.stats import entropy
import random

ITR_TRY = 1000
NAME_PAIR_WEIGHT = 0.3
MAX_VALUE_ENTRIES = 500
MAX_RETURN_ENTRIES = 1000
PAGERANK_QUIT_EPOCH = 20

db_ids_to_ignore = [124, 131]

SETTING = 'spider'

DATABASE_PATH = './spider/spider/database'
TABLE_METADATA_PATH = './spider/spider/tables_mod.json'
SAVE_PATH = './saved_results_spider/'

'''
DATABASE_PATH = './CSpider/database'
TABLE_METADATA_PATH = './CSpider/tables_mod.json'
SAVE_PATH = './saved_results_CSpider'
'''

def transform2distribution_proportional(scores):
	# strip the negative values to zero (the negative values were there in the first hand
	# because of considerations for softmax)
	temp_scores = []
	for item in scores:
		if item < 0:
			temp_scores.append(0)
		else:
			temp_scores.append(item)
	total = float(sum(temp_scores))
	dist = []
	for item in temp_scores:
		dist.append(item / total)
	return dist


transform2distribution = transform2distribution_proportional

PAGERANK_CRITICAL_VALUE = 0.000001
PAGERANK_QVALUE = 0.2  # a baseline score with which probability the traverse jumps to a random node

CALCULABLE_DTYPES = ['int', 'double']
SUBTRACTABLE_DTYPES = CALCULABLE_DTYPES + ['datetime', 'year']
ORDERBY_DTYPES = ['int', 'datetime', 'year', 'double', 'id', 'timestamp']
UNLIKELY_COND_DTYPES = ['timestamp', 'blob',
						'bit']  # those dtypes that are unlikely present in where / having / join-on conditions
ALL_DTYPES = ['int', 'varchar(1000)', 'double', 'id', 'datetime', 'bit', 'bool', 'timestamp', 'year', 'blob']

# the probability of 'in' for CMP_DISTS is constantly zero,
# since those cases with 'in' are covered saperately in function #construct_ci_cdts#

# 1 -> between; 2 -> =; 3 -> >; 4 -> <; 5 -> >=; 6 -> <=; 7 -> !=; 8 -> in; 9 -> like
CMP_DISTS = {'int': transform2distribution_proportional([38, 309, 252, 99, 35, 18, 7, 80, 0]),
			 'double': transform2distribution_proportional([24, 16, 190, 65, 12, 4, 4, 2, 0]),
			 'varchar(1000)': transform2distribution_proportional([0, 2306, 27, 16, 3, 0, 106, 30, 190]),
			 'id': transform2distribution_proportional([2, 63, 2, 0, 0, 0, 4, 166, 0]),
			 'bit': transform2distribution_proportional([0, 2, 0, 0, 0, 0, 0, 0, 0]),
			 'datetime': transform2distribution_proportional([2, 8, 22, 21, 4, 4, 0, 0, 4]),
			 'bool': transform2distribution_proportional([0, 10, 0, 0, 0, 0, 0, 0, 0]),
			 'timestamp': transform2distribution_proportional([0, 1, 0, 0, 0, 0, 1, 1, 0]),
			 'year': transform2distribution_proportional([0, 1, 1, 1, 1, 1, 1, 1, 0]),
			 'blob': transform2distribution_proportional([0, 1, 0, 0, 0, 0, 1, 1, 0])}
# for those types without actual data in SPIDER, assign equal probability to each feasible comparer
# convert into default_dicts in order to be compatible with more database dtypes
CMP_DISTS = defaultdict(lambda: [0, 1, 0, 0, 0, 0, 1, 1, 0], CMP_DISTS)

AGGR_DISTS = {'int': transform2distribution_proportional([0, 1, 1, 2.5, 1, 2]),
			  'double': transform2distribution_proportional([0, 1, 1, 2.5, 1, 2]),
			  'varchar(1000)': transform2distribution_proportional([0, 0, 0, 1, 0, 0]),
			  'id': transform2distribution_proportional([0, 0, 0, 1, 0, 0]),
			  'bit': transform2distribution_proportional([0, 0, 0, 1, 0, 0]),
			  'datetime': transform2distribution_proportional([0, 1, 1, 5, 0, 0]),
			  'bool': transform2distribution_proportional([0, 0, 0, 1, 0, 0]),
			  'timestamp': transform2distribution_proportional([0, 0, 0, 1, 0, 0]),
			  'year': transform2distribution_proportional([0, 1, 1, 5, 0, 0]),
			  'blob': transform2distribution_proportional([0, 0, 0, 1, 0, 0])}

# convert into default_dicts in order to be compatible with more database dtypes
AGGR_DISTS = defaultdict(lambda: [0, 0, 0, 1, 0, 0], AGGR_DISTS)


def transform2distribution_softmax(scores):
	dist = softmax(scores)
	return dist


# a placeholder function
def chinese_split_words(c_chinese):
	return [c_chinese]


def cut_value_for_like(v, mode):
	v = v.strip('"')
	v = v.strip("'")

	len_dist = [0.01] + [1] * (len(v) - 1) + [0.01]
	len_dist[1] += len(v)
	if len(v) - 2 > 1:
		len_dist[2] += len(v)
	if len(v) - 2 > 2:
		len_dist[3] += len(v)
	if len(v) - 2 > 3:
		len_dist[4] += int(len(v) / 2)
	if len(v) - 2 > 4:
		len_dist[5] += int(len(v) / 3)
	len_dist = transform2distribution_proportional(len_dist)
	length = numpy.random.choice(range(len(v) + 1), p=len_dist)
	if length == 0:
		length = 1
	if mode == 'mid':
		start_point_dist = [1] * len(v)
		# favor middle characters to the left a little bit as starting point more
		for idx in range(len(v)):
			if idx == 0 or idx == len(v) - 1:
				start_point_dist[idx] = 0.1
			else:
				start_point_dist[idx] += (idx + 1) * (len(v) - idx)
		start_point_dist = transform2distribution_proportional(start_point_dist)
		start_point = numpy.random.choice(range(len(v)), p=start_point_dist)
		start_point = min(start_point, len(v) - 1)
		length = min(length, len(v) - start_point)
		if length == 0:
			length = 1
		_slice = v[start_point:start_point + length]
		_slice = '%' + _slice + '%'
	elif mode == 'head':
		length = min(length, len(v))
		_slice = v[:length]
		_slice = _slice + '%'
	elif mode == 'tail':
		length = min(length, len(v))
		_slice = v[len(v) - length:]
		_slice = '%' + _slice
	else:
		raise AssertionError
	return _slice


def calc_num_queries_via_stats_regressed(num_rels):
	res = 76 + 3 * num_rels
	assert res > 0
	res = int(min(res, 300))
	return res


calc_num_queries_via_stats = calc_num_queries_via_stats_regressed


def is_comparable(str1, str2):
	if str1 in CALCULABLE_DTYPES and str2 in CALCULABLE_DTYPES:
		return True
	elif 'varchar' in str1 and 'varchar' in str2 and str1 == str2:
		return True
	elif str1 == str2:
		return True
	else:
		return False


def pagerank_scores(matrix, typenps):
	assert len(typenps) == matrix.shape[0]
	matrix = numpy.matrix(matrix)
	assert (matrix.ndim == 2)
	assert (matrix.shape[0] == matrix.shape[1])
	last_scores = [len(tp.properties) for tp in typenps]
	last_scores = [x*len(last_scores)/sum(last_scores) for x in last_scores]  # normalize average to 1
	weightsums = matrix.sum(axis=1, dtype='float').flatten()
	_iter = 0
	while True:
		scores = [PAGERANK_QVALUE] * matrix.shape[0]
		for i in range(matrix.shape[0]):
			for j in range(matrix.shape[1]):
				# divide score with total sum of weights of the source table
				scores[j] += ((1 - PAGERANK_QVALUE) * matrix[i, j] * last_scores[i]) / weightsums[0, i]
		if last_scores is None:
			continue
		assert (len(last_scores) == len(scores))
		# check whether the scores are stable at this round
		stable = True
		for a, b in zip(last_scores, scores):
			if abs(a - b) > PAGERANK_CRITICAL_VALUE:
				stable = False
				break
		if stable or _iter > PAGERANK_QUIT_EPOCH:
			if _iter > PAGERANK_QUIT_EPOCH:
				print("!")
			break
		else:
			last_scores = scores
			_iter += 1
	return last_scores


# table_ids means the previous tables already selected into this query
def calc_importance(typenps, type_mat, fk_rels, table_ids, prior_scores=None):
	if len(table_ids) == 0:
		fk_reverse_weight_mat = numpy.zeros_like(type_mat)
		for rel in fk_rels:
			fk_reverse_weight_mat[rel.table_ids[0], rel.table_ids[1]] += 0.3
		pr_mat = type_mat + fk_reverse_weight_mat
		scores = pagerank_scores(pr_mat, typenps)
	else:
		assert (prior_scores is not None)
		scores = [0] * type_mat.shape[0]
		# for each existing table in query, add its influence matrix
		# adjusted with its own prior importance to the scoring of next table
		for idx_1 in table_ids:
			for idx_2 in range(len(type_mat)):
				# the direction of type-mat is pk -> fk
				scores[idx_2] += type_mat[idx_2, idx_1] * prior_scores[idx_1]
	return scores


# use entropy as a criterion for group by
def calc_group_score(dtype, values):
	if values is None:
		return 0
	value_buckets = {}
	for val in values:
		if val.z not in value_buckets:
			value_buckets[val.z] = 1
		else:
			value_buckets[val.z] += 1
	value_dist = []
	for val_z in value_buckets:
		value_dist.append(float(value_buckets[val_z]) / len(values))
	score = entropy(value_dist)
	return score


def choose_groupby_prop(groupable_prop_ids, propertynps, num_groupbys):
	group_scores = []
	for prop_id in groupable_prop_ids:
		group_scores.append(propertynps[prop_id].group_score)
	group_dist = transform2distribution(group_scores)
	num_groupbys = min(num_groupbys, len(groupable_prop_ids))
	try:
		propids_for_group = numpy.random.choice(groupable_prop_ids, num_groupbys, replace=False, p=group_dist)
	except Exception as e:
		print(e)
		raise
	available_after_group_by_prop_ids = find_available_propids_after_groupby(propertynps, propids_for_group)
	return propids_for_group, available_after_group_by_prop_ids


def find_available_propids_after_groupby(propertynps, propids_for_group):
	available_after_group_by_prop_ids = []
	for prop_id in propids_for_group:
		available_after_group_by_prop_ids.append(prop_id)
		# for the unique properties (such as id / name), all other unique properties
		# would also be available to go with no aggregators
		if propertynps[prop_id].is_unique:
			for idx, prop_j in enumerate(propertynps):
				if prop_j.table_id == propertynps[prop_id].table_id and prop_j.is_unique:
					available_after_group_by_prop_ids.append(idx)
	return available_after_group_by_prop_ids


def calc_forquery_distribution(available_prop_ids, groupby_prop_ids, propertynps):
	assert available_prop_ids is not None
	assert len(available_prop_ids) > 0
	# average probability of appearance as being queried is set according to stats from SPIDER
	scores = [0] * len(propertynps)
	for idx in available_prop_ids:
		if propertynps[idx].dtype == 'id':
			scores[idx] = 0.15
		elif propertynps[idx].dtype in ['datetime', 'int']:
			scores[idx] = 0.5
		elif propertynps[idx].dtype == 'double':
			scores[idx] = 0.7
		elif 'varchar' in propertynps[idx].dtype:
			scores[idx] = 1.7
		else:
			scores[idx] = 1
	if groupby_prop_ids is not None:
		for idx in groupby_prop_ids:
			scores[idx] += 5
	scores = transform2distribution_proportional(scores)
	return scores


#	values	->	VALUENP
def is_unique(values):
	for idx_1, val_1 in enumerate(values):
		for idx_2, val_2 in enumerate(values):
			if idx_1 == idx_2:
				continue
			is_same = True
			if isinstance(val_1, list) or isinstance(val_1, tuple):
				assert isinstance(val_1, list) or isinstance(val_1, tuple)
				assert len(val_1) == len(val_2)
				for idx in range(len(val_1)):
					if val_1[idx] != val_2[idx]:
						is_same = False
						break
			else:
				if val_1 != val_2:
					is_same = False
			if is_same:
				return False
	return True


def all_the_same(lst):
	bucket = []
	for item in lst:
		assert isinstance(item, list) or isinstance(item, tuple)
		appeared = False
		for j in bucket:
			assert len(item) == len(j)
			is_same = True
			for idx in range(len(item)):
				if item[idx] != j[idx]:
					is_same = False
					break
			if is_same:
				appeared = True
				break

		if not appeared:
			bucket.append(item)
	if len(bucket) <= 1:
		return True
	elif len(bucket) > 1:
		return False


def either_contain(res_1, res_2):
	bucket_both = []
	bucket_1 = []
	bucket_2 = []
	for val_1 in res_1:
		assert isinstance(val_1, list) or isinstance(val_1, tuple)
		appeared = False
		for val_2 in bucket_1:
			is_same = True
			for idx in range(len(val_1)):
				if val_1[idx] != val_2[idx]:
					is_same = False
					break
			if is_same:
				appeared = True
				break
		if not appeared:
			bucket_1.append(val_1)

	for val_1 in res_2:
		assert isinstance(val_1, list) or isinstance(val_1, tuple)
		appeared = False
		for val_2 in bucket_2:
			is_same = True
			for idx in range(len(val_1)):
				if val_1[idx] != val_2[idx]:
					is_same = False
					break
			if is_same:
				appeared = True
				break
		if not appeared:
			bucket_2.append(val_1)

	for val_1 in res_1:
		assert isinstance(val_1, list) or isinstance(val_1, tuple)
		appeared = False
		for val_2 in bucket_both:
			is_same = True
			for idx in range(len(val_1)):
				if val_1[idx] != val_2[idx]:
					is_same = False
					break
			if is_same:
				appeared = True
				break
		if not appeared:
			bucket_both.append(val_1)

	for val_1 in res_2:
		assert isinstance(val_1, list) or isinstance(val_1, tuple)
		appeared = False
		for val_2 in bucket_both:
			is_same = True
			for idx in range(len(val_1)):
				if val_1[idx] != val_2[idx]:
					is_same = False
					break
			if is_same:
				appeared = True
				break
		if not appeared:
			bucket_both.append(val_1)
	if len(bucket_1) == len(bucket_both) or len(bucket_2) == len(bucket_both):
		return True
	else:
		return False


def choose_proppair_for_cdt(left_available_prop_ids, right_available_prop_ids, propertynps, prop_mat):
	left_candidate_ids = []
	for idx in left_available_prop_ids:
		if propertynps[idx].dtype not in UNLIKELY_COND_DTYPES:
			left_candidate_ids.append(idx)

	right_candidate_ids = []
	for idx in right_available_prop_ids:
		if propertynps[idx].dtype not in UNLIKELY_COND_DTYPES:
			right_candidate_ids.append(idx)

	cand_prop_mat = []
	for i in left_candidate_ids:
		for j in right_candidate_ids:
			if prop_mat[i, j] == 0 or i == j or (is_comparable(propertynps[i].dtype, propertynps[j].dtype) is False):
				cand_prop_mat.append(-1000000)
			elif propertynps[i].dtype == 'id' and propertynps[j].dtype == 'id':
				cand_prop_mat.append(prop_mat[i, j] * 0.3)
			else:
				cand_prop_mat.append(prop_mat[i, j])
	try:
		cand_prop_dist = transform2distribution(cand_prop_mat)
	except Exception as e:
		return None, None
	pair_id = numpy.random.choice(numpy.arange(len(cand_prop_mat)), p=cand_prop_dist)
	left_id = left_candidate_ids[int(pair_id / len(right_candidate_ids))]
	right_id = right_candidate_ids[pair_id % len(right_candidate_ids)]
	return left_id, right_id


def list_recursively_same(l1, l2):
	if len(l1) != len(l2):
		return False
	for idx in range(len(l1)):
		if isinstance(l1[idx], list) and isinstance(l2[idx], list):
			if not list_recursively_same(l1[idx], l2[idx]):
				return False
		elif isinstance(l1[idx], list) or isinstance(l2[idx],list):
			return False
		elif isinstance(l1[idx], dict) and isinstance(l2[idx], dict):
			if not sql_is_same(l1[idx], l2[idx]):
				return False
		else:
			if l1[idx] != l2[idx]:
				return False
	return True


def sql_is_same(qry1, qry2):
	if qry1['except'] is None and qry2['except'] is None:
		pass
	elif qry1['except'] is None or qry2['except'] is None:
		return False
	else:
		except_same = sql_is_same(qry1['except'], qry2['except'])
		if not except_same:
			return False

	if not list_recursively_same(qry1['from']['conds'], qry2['from']['conds']):
		return False
	if not list_recursively_same(qry1['from']['table_units'], qry2['from']['table_units']):
		return False

	if not list_recursively_same(qry1['groupBy'], qry2['groupBy']):
		return False
	if not list_recursively_same(qry1['having'], qry2['having']):
		return False

	if qry1['intersect'] is None and qry2['intersect'] is None:
		pass
	elif qry1['intersect'] is None or qry2['intersect'] is None:
		return False
	else:
		intersect_same = sql_is_same(qry1['intersect'], qry2['intersect'])
		if not intersect_same:
			return False

	if qry1['limit'] is None and qry2['limit'] is None:
		pass
	elif qry1['limit'] is None or qry2['limit'] is None:
		return False
	else:
		if qry1['limit'] != qry2['limit']:
			return False

	if not list_recursively_same(qry1['orderBy'], qry2['orderBy']):
		return False
	if not list_recursively_same(qry1['select'], qry2['select']):
		return False

	if qry1['union'] is None and qry2['union'] is None:
		pass
	elif qry1['union'] is None or qry2['union'] is None:
		return False
	else:
		union_same = sql_is_same(qry1['union'], qry2['union'])
		if not union_same:
			return False

	if not list_recursively_same(qry1['where'], qry2['where']):
		return False

	'''
	assert qry1['db_id'] == qry2['db_id']
	if len(qry1['query_toks_no_value']) != len(qry2['query_toks_no_value']):
		return False
	tok1 = [item.lower() for item in qry1['query_toks_no_value']]
	tok2 = qry2['query_toks_no_value']
	for idx in range(len(tok1)):
		if tok1[idx].lower() != tok2[idx].lower():
			return False
	'''
	return True


def solve_ztok_aggr(z_toks):
	washed_ztoks = []
	for item in z_toks:
		if len(item) > 0:
			washed_ztoks.append(item)
	z_toks = washed_ztoks

	tok_idx = 0
	new_ztoks = []
	aggregator_present = False
	while tok_idx < len(z_toks):
		item = z_toks[tok_idx]
		item_splitted = []
		if 'avg(' in item.lower():
			aggregator_present = True
			item_splitted += ['avg', '(']
			if item[-1] == ')':
				item_splitted.append(item[4:-1])
				item_splitted.append(')')
			else:
				item_splitted.append(item[4:])
				tok_idx += 1
				lapsed = 1
				while z_toks[tok_idx][-1] != ')':
					item_splitted.append(z_toks[tok_idx])
					tok_idx += 1
					lapsed += 1
				# assert that a count(xxx) clause does not contain more than 2 spaces within the paratheses
				if lapsed > 4:
					print(z_toks)
					raise AssertionError
				item_splitted.append(z_toks[tok_idx][:-1])
				item_splitted.append(')')

		elif 'max(' in item.lower():
			aggregator_present = True
			item_splitted += ['max', '(']
			if item[-1] == ')':
				item_splitted.append(item[4:-1])
				item_splitted.append(')')
			else:
				item_splitted.append(item[4:])
				tok_idx += 1
				lapsed = 1
				while z_toks[tok_idx][-1] != ')':
					item_splitted.append(z_toks[tok_idx])
					tok_idx += 1
					lapsed += 1
				# assert that a count(xxx) clause does not contain more than 2 spaces within the paratheses
				if lapsed > 4:
					print(z_toks)
					raise AssertionError
				item_splitted.append(z_toks[tok_idx][:-1])
				item_splitted.append(')')

		elif 'min(' in item.lower():
			aggregator_present = True
			item_splitted += ['min', '(']
			if item[-1] == ')':
				item_splitted.append(item[4:-1])
				item_splitted.append(')')
			else:
				item_splitted.append(item[4:])
				tok_idx += 1
				lapsed = 1
				while z_toks[tok_idx][-1] != ')':
					item_splitted.append(z_toks[tok_idx])
					tok_idx += 1
					lapsed += 1
				# assert that a count(xxx) clause does not contain more than 2 spaces within the paratheses
				if lapsed > 4:
					print(z_toks)
					raise AssertionError
				item_splitted.append(z_toks[tok_idx][:-1])
				item_splitted.append(')')

		elif 'sum(' in item.lower():
			aggregator_present = True
			item_splitted += ['sum', '(']
			if item[-1] == ')':
				item_splitted.append(item[4:-1])
				item_splitted.append(')')
			else:
				item_splitted.append(item[4:])
				tok_idx += 1
				lapsed = 1
				while z_toks[tok_idx][-1] != ')':
					item_splitted.append(z_toks[tok_idx])
					tok_idx += 1
					lapsed += 1
				# assert that a count(xxx) clause does not contain more than 2 spaces within the paratheses
				if lapsed > 4:
					print(z_toks)
					raise AssertionError
				item_splitted.append(z_toks[tok_idx][:-1])
				item_splitted.append(')')

		elif 'count(' in item.lower():
			aggregator_present = True
			item_splitted += ['count', '(']
			if item[-1] == ')':
				item_splitted.append(item[6:-1])
				item_splitted.append(')')
			else:
				item_splitted.append(item[6:])
				tok_idx += 1
				lapsed = 1
				while z_toks[tok_idx][-1] != ')':
					item_splitted.append(z_toks[tok_idx])
					tok_idx += 1
					lapsed += 1
				# assert that a count(xxx) clause does not contain more than 2 spaces within the paratheses
				if lapsed > 5:
					print(z_toks)
					raise AssertionError
				item_splitted.append(z_toks[tok_idx][:-1])
				item_splitted.append(')')

		else:
			item_splitted.append(item)

		new_ztoks += item_splitted
		tok_idx += 1

	if aggregator_present:
		print("New z-toks: ")
		print(new_ztoks)

	return new_ztoks


def sql_features(sql_struct):
	features = []
	# len1, len2, len3, len4, hasStar, hasMax, hasMin, hasCount, hasSum, hasAvg
	select_feat = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
	if len(sql_struct['select'][1]) == 1:
		select_feat[0] = 1
	elif len(sql_struct['select'][1]) == 2:
		select_feat[1] = 1
	elif len(sql_struct['select'][1]) == 3:
		select_feat[2] = 1
	elif len(sql_struct['select'][1]) > 3:
		select_feat[3] = 1
	else:
		raise AssertionError
	for item in sql_struct['select'][1]:
		if item[1][1][1] == 0:
			select_feat[4] = 4
		if item[1][1][0] == 1:
			select_feat[5] = 2
		elif item[1][1][0] == 2:
			select_feat[6] = 2
		elif item[1][1][0] == 3:
			select_feat[7] = 2
		elif item[1][1][0] == 4:
			select_feat[8] = 2
		elif item[1][1][0] == 5:
			select_feat[9] = 2
		elif item[1][1][0] == 0:
			pass
		else:
			raise AssertionError
	features += select_feat

	from_feat = [0, 0, 0, 0, 0] # len1, len2, len3, len4, join-equal-to-len_minus_1
	if len(sql_struct['from']['table_units']) == 1:
		from_feat[0] = 1
	elif len(sql_struct['from']['table_units']) == 2:
		from_feat[1] = 1
	elif len(sql_struct['from']['table_units']) == 3:
		from_feat[2] = 1
	elif len(sql_struct['from']['table_units']) > 3:
		from_feat[3] = 1
	else:
		raise AssertionError
	if len(sql_struct['from']['conds']) == (len(sql_struct['from']['table_units'])-1):
		from_feat[4] = 2
	features += from_feat

	# len0, len1, len2, len3, len4, hasStar, hasMax, hasMin, hasCount, hasSum, hasAvg, hasSubq, has_col
	# hasBetween, hasEqual, hasLT, hasST, hasNLT, hasNST, hasNE, hasIn, hasLike
	where_feat = [0, 0, 0, 0, 0,
				  0, 0, 0, 0, 0, 0, 0, 0,
				  0, 0, 0, 0, 0, 0, 0, 0, 0]

	if len(sql_struct['where']) == 0:
		where_feat[0] = 2
	elif len(sql_struct['where']) == 1:
		where_feat[1] = 1
	elif len(sql_struct['where']) == 2:
		where_feat[2] = 1
	elif len(sql_struct['where']) == 3:
		where_feat[3] = 1
	elif len(sql_struct['where']) > 3:
		where_feat[4] = 1
	else:
		raise AssertionError

	for item in sql_struct['where']:
		if isinstance(item, str):
			continue
		if item[2][1][1] == 0:
			where_feat[5] = 4
		if item[2][1][0] == 0:
			pass
		elif item[2][1][0] == 1:
			where_feat[6] = 1
		elif item[2][1][0] == 2:
			where_feat[7] = 1
		elif item[2][1][0] == 3:
			where_feat[8] = 1
		elif item[2][1][0] == 4:
			where_feat[9] = 1
		elif item[2][1][0] == 5:
			where_feat[10] = 1
		else:
			raise AssertionError
		if isinstance(item[3], dict):
			where_feat[11] = 4
		elif isinstance(item[3], list):
			where_feat[12] = 4

		if item[1] == 1:
			where_feat[13] = 1
		elif item[1] == 2:
			where_feat[14] = 1
		elif item[1] == 3:
			where_feat[15] = 1
		elif item[1] == 4:
			where_feat[16] = 1
		elif item[1] == 5:
			where_feat[17] = 1
		elif item[1] == 6:
			where_feat[18] = 1
		elif item[1] == 7:
			where_feat[19] = 1
		elif item[1] == 8:
			where_feat[20] = 1
		elif item[1] == 9:
			where_feat[21] = 1
		else:
			raise AssertionError
	features += where_feat

	gb_feat = [0, 0, 0, 0, 0, 0]  # len0,len1, len2, len3, has_having, having_subq
	if len(sql_struct['groupBy']) == 0:
		gb_feat[0] = 1
	elif len(sql_struct['groupBy']) == 1:
		gb_feat[1] = 1
	elif len(sql_struct['groupBy']) == 2:
		gb_feat[2] = 1
	elif len(sql_struct['groupBy']) > 2:
		gb_feat[3] = 1

	if len(sql_struct['having']) > 0:
		gb_feat[4] = 2
	for item in sql_struct['having']:
		if isinstance(item, str):
			continue
		if isinstance(item[3], dict):
			gb_feat[5] = 2
	features += gb_feat

	ob_feat = [0, 0, 0, 0, 0, 0, 0, 0]  # len0, len1, len2, len3, asc, desc, has_limit, limit 1
	if len(sql_struct['orderBy']) == 0:
		ob_feat[0] = 1
	elif len(sql_struct['orderBy'][1]) == 1:
		ob_feat[1] = 1
	elif len(sql_struct['orderBy'][1]) == 2:
		ob_feat[2] = 1
	elif len(sql_struct['orderBy'][1]) > 2:
		ob_feat[3] = 1
	else:
		raise AssertionError
	if len(sql_struct['orderBy']) > 0 and sql_struct['orderBy'][0] == 'asc':
		ob_feat[4] = 1
	elif len(sql_struct['orderBy']) > 0 and sql_struct['orderBy'][0] == 'desc':
		ob_feat[5] = 1
	else:
		pass
	if sql_struct['limit'] is not None:
		ob_feat[6] = 1
		if sql_struct['limit'] == 1:
			ob_feat[7] = 2
	features += ob_feat

	post_feat = [0, 0, 0]  # union, except, intersect

	if sql_struct['union'] is not None:
		vec_2 = sql_features(sql_struct['union'])
		post_feat[0] = 4
		vec_2[0][-3] = 4
	elif sql_struct['except'] is not None:
		vec_2 = sql_features(sql_struct['except'])
		post_feat[1] = 4
		vec_2[0][-2] = 4
	elif sql_struct['intersect'] is not None:
		vec_2 = sql_features(sql_struct['intersect'])
		post_feat[2] = 4
		vec_2[0][-1] = 4
	else:
		vec_2 = None
	features += post_feat
	features = [features]
	if vec_2 is not None:
		features += vec_2
	return features


def fetch_refs(res_json, ref_json, lang, same=False):
	ret_json = []
	# add references to jsons
	for key_idx, key_dict in enumerate(res_json):
		key_vecs = [numpy.array(v, dtype=numpy.float) for v in key_dict['sql_vec']]

		# distances only contain distances within 5, if this list returns less than 10 items, use distances_backup instead
		distances = []
		distances_secondary = []
		distances_backup = []
		if key_idx % 100 == 1:
			print(key_idx)
		for val_idx, val_dict in enumerate(ref_json):
			if same and val_idx == key_idx:
				continue
			val_vecs = [numpy.array(v, dtype=numpy.float) for v in val_dict['sql_vec']]
			min_dist = float('inf')
			for kv in key_vecs:
				for vv in val_vecs:
					# use norm-1 instead of norm-2 to avoid assigning to much credit to larger weighted features
					cur_dist = numpy.linalg.norm(kv-vv, ord=1)
					if cur_dist < min_dist:
						min_dist = cur_dist
			distances_backup.append((min_dist, val_idx))
			if min_dist <= 1.2:
				distances.append((min_dist, val_idx))
			if min_dist <= 3:
				distances_secondary.append((min_dist, val_idx))
		# print(len(distances))
		# use two layer cache
		if len(distances) < 10:
			distances = distances_secondary
		if len(distances) < 10:
			distances = distances_backup
		distances = sorted(distances, key=lambda x: x[0])
		# randomly choose one from top 3 most similar SQL queries
		ref_idx = random.choice(distances[:10])[1]
		if lang == 'eng':
			ref_sequence = ref_json[ref_idx]['question_sequence']
		elif lang == 'chi':
			ref_sequence = ref_json[ref_idx]['question_sequence_chinese']
		else:
			raise AssertionError
		ref_gold = ref_json[ref_idx]['question_gold']
		ref_response = ref_json[ref_idx]['answer_sample']
		key_dict['ref_question_sequence'] = ref_sequence
		key_dict['ref_gold'] = ref_gold
		key_dict['ref_response'] = ref_response
		ret_json.append(key_dict)
	assert len(res_json) == len(ret_json)
	return ret_json
