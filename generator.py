"""
# -*- coding: latin_1 -*-
"""

import time
import os
import argparse
from core import *
from load_db import *


parser = argparse.ArgumentParser()
parser.add_argument('-d', '--db_id', type=int, default=0)
parser.add_argument('-v', '--verbose', type=bool, default=False, help='print verbose info')
parser.add_argument('-m', '--mode', type=str, default='run', help='which mode to run')
parser.add_argument('-s', '--start_from', type=int, default=0, help='start_from for #convert# mode')
parser.add_argument('-g', '--gold_path', type=str, default='./spider/spider/train_spider_corrected.json')
args = parser.parse_args()

tableid = 0
propertyid = 0


def format_sql_spider(np):
	res = {}
	if np.has_except is True:
		res['except'] = format_sql_spider(np.np_2)
		res['union'] = None
		res['intersect'] = None
	elif np.has_union is True:
		res['except'] = None
		res['union'] = format_sql_spider(np.np_2)
		res['intersect'] = None
	elif np.has_intersect is True:
		res['except'] = None
		res['union'] = None
		res['intersect'] = format_sql_spider(np.np_2)
	else:
		res['except'] = None
		res['union'] = None
		res['intersect'] = None

	res['from'] = {}
	conds = []
	table_units = []
	for item in np.join_cdts:
		assert item.cmper.negative is False
		assert int(item.left.aggr) == 0
		assert int(item.right.aggr) == 0
		conds.append(
			[False, item.cmper.index, [0, [0, item.left.meta_idx + 1, False], None],
			 [0, item.right.meta_idx + 1, False], None])
	for item in np.table_ids:
		table_units.append(['table_unit', int(item)])
	res['from']['conds'] = conds
	res['from']['table_units'] = table_units

	res['groupBy'] = []
	for item in np.group_props:
		res['groupBy'].append([0, item.meta_idx + 1, False])

	res['having'] = []
	if np.having_cdts is not None and len(np.having_cdts) > 0:

		for hv_idx, item in enumerate(np.having_cdts):
			assert item.cmper.negative is False
			hvg = [False, item.cmper.index, [0, [int(item.left.aggr), item.left.meta_idx + 1,
														  item.left.distinct], None]]
			# condition type must be  either cv or ci
			if isinstance(item.right, list) or isinstance(item.right, numpy.ndarray):
				if len(item.right) == 1:
					if item.left.dtype in ['int', 'star']:
						hvg.append(int(float(item.right[0].z)))
					elif item.left.dtype == 'double':
						hvg.append(float(item.right[0].z))
					else:
						hvg.append(item.right[0].z)
					hvg.append(None)
				else:
					assert (len(item.right) == 2)
					if item.left.dtype in ['int', 'star']:
						hvg.append(int(float(item.right[0].z)))
						hvg.append(int(float(item.right[1].z)))
					elif item.left.dtype == 'double':
						hvg.append(float(item.right[0].z))
						hvg.append(float(item.right[1].z))
					else:
						hvg.append(item.right[0].z)
						hvg.append(item.right[1].z)
			else:
				assert (isinstance(item.right, QRYNP))
				hvg.append(format_sql_spider(item.right.np))
				hvg.append(None)
			res['having'].append(hvg)

	if np.limit is not None:
		res['limit'] = int(np.limit)
	else:
		res['limit'] = None

	if np.orderby_order is None:
		res['orderBy'] = []
	else:
		res['orderBy'] = [np.orderby_order]
		orderby_proplist = []
		for prop in np.orderby_props:
			orderby_proplist.append([0, [int(prop.aggr), prop.meta_idx + 1, False], None])
		res['orderBy'].append(orderby_proplist)

	res['select'] = [np.distinct]
	queried_proplist = []
	for prop in np.queried_props:
		queried_proplist.append([int(prop.aggr), [0, [0, prop.meta_idx + 1, prop.distinct], None]])
	res['select'].append(queried_proplist)
	'''
	try:
		string = json.dumps(res)
	except Exception as e:
		raise AssertionError
	'''
	res['where'] = []
	for cond_idx, cond in enumerate(np.cdts):
		# convert the condition into a form of list same as SPIDER
		listed_cond = [cond.cmper.negative, cond.cmper.index,
					   [0, [0, cond.left.meta_idx + 1, False], None]]
		if isinstance(cond.right, QRYNP):
			listed_cond.append(format_sql_spider(cond.right.np))
			listed_cond.append(None)
		elif isinstance(cond.right, PROPERTYNP):
			listed_cond.append([0, cond.right.meta_idx + 1, False])
			listed_cond.append(None)
		else:
			if not isinstance(cond.right, list) and not isinstance(cond.right, numpy.ndarray):
				print(cond.right)
				raise AssertionError
			if len(cond.right) == 1:
				if cond.left.dtype in ['int', 'star']:
					#listed_cond.append(int(float(cond.right[0].z.strip('"').strip("'"))))
					listed_cond.append(cond.right[0].z.strip('"').strip("'"))
				elif cond.left.dtype == 'double':
					#listed_cond.append(float(cond.right[0].z.strip('"').strip("'")))
					listed_cond.append(cond.right[0].z.strip('"').strip("'"))
				else:
					listed_cond.append(cond.right[0].z)
				listed_cond.append(None)
			elif len(cond.right) == 2:
				if cond.left.dtype in ['int', 'star']:
					#listed_cond.append(int(float(cond.right[0].z.strip('"').strip("'"))))
					#listed_cond.append(int(float(cond.right[1].z.strip('"').strip("'"))))
					listed_cond.append(cond.right[0].z.strip('"').strip("'"))
					listed_cond.append(cond.right[1].z.strip('"').strip("'"))
				elif cond.left.dtype == 'double':
					#listed_cond.append(float(cond.right[0].z.strip('"').strip("'")))
					#listed_cond.append(float(cond.right[1].z.strip('"').strip("'")))
					listed_cond.append(cond.right[0].z.strip('"').strip("'"))
					listed_cond.append(cond.right[1].z.strip('"').strip("'"))
				else:
					listed_cond.append(cond.right[0].z)
					listed_cond.append(cond.right[1].z)
			else:
				raise AssertionError
		res['where'].append(listed_cond)
		if cond_idx < len(np.cdt_linkers):
			res['where'].append(np.cdt_linkers[cond_idx])
	return res


def format_query_to_spider(np, qrynp, database_name, sample, headers):
	qry_formatted = {}
	qry_formatted['db_id'] = database_name
	qry_formatted['query'] = qrynp.z
	qry_formatted['query_toks'] = qrynp.z_toks
	qry_formatted['query_toks_no_value'] = qrynp.z_toks_novalue
	qry_formatted['question'] = qrynp.c_english
	qry_formatted['question_toks'] = qrynp.c_english.split()
	for idx in range(len(qry_formatted['question_toks'])):
		qry_formatted['question_toks'][idx] = qry_formatted['question_toks'][idx].strip('$')
	qry_formatted['question_sequence'] = qrynp.c_english_sequence
	qry_formatted['question_sequence_toks'] = [item.split() for item in qrynp.c_english_sequence]
	qry_formatted['question_chinese'] = qrynp.c_chinese
	qry_formatted['question_toks_chinese'] = chinese_split_words(qrynp.c_chinese)
	qry_formatted['question_sequence_chinese'] = qrynp.c_chinese_sequence
	qry_formatted['questions_sequence_toks_chinese'] = [chinese_split_words(item) for item in qrynp.c_chinese_sequence]
	qry_formatted['sql'] = format_sql_spider(np)
	serialized_sample = [', '.join(headers)]
	for s in sample:
		assert len(s) == len(headers)
		serialized_s = []
		for c in s:
			try:
				serialized_s.append(str(c))
			except Exception as e:
				serialized_s.append('#Unsupported#')
		serialized_sample.append(', '.join(serialized_s))
	qry_formatted['answer_sample'] = serialized_sample
	assert 'limit' in qry_formatted['sql']
	return qry_formatted


def generate_queries(database_idx, verbose):
	database_path, database_name, typenps, propertynps, type_matrix, property_matrix, prop_rels, fk_rels, valid_database, conn, crsr, num_queries = build_spider_dataset(
		database_idx)
	if not valid_database:
		return [], [], [], [], [], []

	saved_results = []
	dumped_results = []
	saved_gold = []
	dumped_gold = []
	saved_canonical = []
	dumped_canonical = []
	if not os.path.isdir(SAVE_PATH + database_name + '/'):
		os.makedirs(SAVE_PATH + database_name + '/')
	saved_gold_path = SAVE_PATH + database_name + '/gold_saved.sql'
	dumped_gold_path = SAVE_PATH + database_name + '/gold_dumped.sql'
	saved_canonical_path = SAVE_PATH + database_name + '/canonical_saved.txt'
	dumped_canonical_path = SAVE_PATH + database_name + '/canonical_dumped.txt'
	saved_gold_fp = open(saved_gold_path, 'w')
	dumped_gold_fp = open(dumped_gold_path, 'w')
	saved_canonical_fp = open(saved_canonical_path, 'w')
	dumped_canonical_fp = open(dumped_canonical_path, 'w')
	error_logfile = open(SAVE_PATH + database_name + '/error.log', 'w')
	print('total number: ', num_queries)
	for query_idx in range(num_queries):
		if query_idx % 10 == 0:
			print(query_idx)
		should_dump = False
		np, qrynp = scratch_build(typenps, propertynps, type_matrix, property_matrix, prop_rels, fk_rels, finalize_sequence=True,
								  cursor=crsr, print_verbose=verbose)
		try:
			qry_returned = crsr.execute(qrynp.z).fetchall()
		except sqlite3.OperationalError as e:
			error_logfile.write('SQL line: ' + qrynp.z + '\n' + 'Error message: ' + str(e) + '\n\n')
			print("!!!: ", qrynp.z, ';\t', e)
			qry_returned = []

		if len(qry_returned) == 0:
			sample_results = []
		else:
			sample_results = [random.choice(qry_returned)]
		headers = [tup[0] for tup in crsr.description]

		if len(qry_returned) == 0:
			should_dump = True
		qry_formatted = format_query_to_spider(np, qrynp, database_name, sample=sample_results, headers=headers)
		try:
			sel = qry_formatted['sql']['orderBy']
			string = json.dumps(sel)
		except Exception as e:
			print(e)
			raise
		if should_dump:
			dumped_results.append(qry_formatted)
			dumped_gold_fp.write(qrynp.z + '\n')
			dumped_gold.append(qrynp.z + '\t' + database_name)
			dumped_canonical_fp.write(qrynp.c_english + '\n\n')
			dumped_canonical.append('In database ' + database_name + ': ' + qrynp.c_english)
		else:
			saved_results.append(qry_formatted)
			saved_gold_fp.write(qrynp.z + '\n')
			saved_gold.append(qrynp.z + '\t' + database_name)
			saved_canonical_fp.write(qrynp.c_english + '\n\n')
			saved_canonical.append('In database ' + database_name + ': ' + qrynp.c_english)
	crsr.close()
	conn.close()
	saved_path = SAVE_PATH + database_name + '/qrys_saved.json'
	dumped_path = SAVE_PATH + database_name + '/qrys_dumped.json'

	with open(saved_path, 'w') as fp:
		json.dump(saved_results, fp, indent=4)

	with open(dumped_path, 'w') as fp:
		json.dump(saved_results, fp, indent=4)

	saved_gold_fp.close()
	dumped_gold_fp.close()
	saved_canonical_fp.close()
	dumped_canonical_fp.close()
	error_logfile.close()
	print("finished generating {0} query entries from database $ {1} $".format(num_queries, database_name))
	return saved_results, dumped_results, saved_gold, dumped_gold, saved_canonical, dumped_canonical


def main(idx, verbose):
	# generate queries for English
	if idx in db_ids_to_ignore:
		print("database number %d ignored!" % idx)
	print("began generating query entries for database $ {0} $".format(idx))
	res_saved, res_dumped, gold_saved, gold_dumped, canonical_saved, canonical_dumped = generate_queries(idx, verbose)
	with open(SAVE_PATH + 'gold_saved.sql', 'a') as fp:
		for line in gold_saved:
			fp.write(line + '\n')
	with open(SAVE_PATH + 'gold_dumped.sql', 'a') as fp:
		for line in gold_dumped:
			fp.write(line + '\n')
	with open(SAVE_PATH + 'canonical_saved.txt', 'a') as fp:
		for line in canonical_saved:
			fp.write(line + '\n\n')
	with open(SAVE_PATH + 'canonical_dumped.txt', 'a') as fp:
		for line in canonical_dumped:
			fp.write(line + '\n\n')

	# here use 'a' in order to be able to create this file at the first run
	try:
		with open(SAVE_PATH + 'qrys_saved.json', 'r') as fp:
			f = json.load(fp)
			assert isinstance(f, list)
			f += res_saved
	except Exception as e:
		print('Exception at saved: ', e)
		f = res_saved
	with open(SAVE_PATH + 'qrys_saved.json', 'w') as fp:
		json.dump(f, fp, indent=4)
	try:
		with open(SAVE_PATH + 'qrys_dumped.json', 'r') as fp:
			f = json.load(fp)
			assert isinstance(f, list)
			f += res_dumped
	except Exception as e:
		print('Exception at dumped: ', e)
		f = res_dumped
	with open(SAVE_PATH + 'qrys_dumped.json', 'w') as fp:
		json.dump(f, fp, indent=4)


def debug(verbose):
	database_path, database_name, tnps, pnps, type_m, property_m, prop_r, fk_rels, valid, conn, crsr, num_queries = build_spider_dataset(
		1)
	assert valid
	fp = open('./result_0131.jsonl', 'w')
	for i in range(ITR_TRY):
		np, qrynp = scratch_build(tnps, pnps, type_m, property_m, prop_r, fk_rels, finalize_sequence=True, cursor=crsr,
								  print_verbose=verbose)

		print('SQL: ', qrynp.z)
		print("Full question: ", qrynp.c_chinese)
		for item in qrynp.c_english_sequence:
			print(item)
		print("")
		fp.write(qrynp.z + '\n')
		try:
			res = crsr.execute(qrynp.z).fetchall()
			headers = [tup[0] for tup in crsr.description]
		except Exception as e:
			print(qrynp.z)
			print(e)
			raise
		qry_formatted = format_query_to_spider(np, qrynp, database_name, sample=res[:3], headers=headers)
	crsr.close()
	conn.close()
	fp.close()


def hit(db_idx, max_iter, verbose):
	database_path, database_name, tnps, pnps, type_m, property_m, prop_r, fk_rels, valid, conn, crsr, num_queries = build_spider_dataset(
		db_idx)
	assert valid

	entries_to_hit = []
	already_hit = []

	with open('/Users/teddy/Files/spider/spider/train_spider.json', 'r') as fp:
		gold_jsons = json.load(fp)
		for item in gold_jsons:
			if item['db_id'] == database_name:
				entries_to_hit.append(item)
				already_hit.append(False)

	total_length = len(entries_to_hit)
	hit_length = 0
	turns_since_last_hit = 0

	for i in range(max_iter):
		np, qrynp = scratch_build(tnps, pnps, type_m, property_m, prop_r, fk_rels, finalize_sequence=True, cursor=crsr,
								  print_verbose=verbose)
		qry_formatted = format_query_to_spider(np, qrynp, database_name, sample=[], headers=[])
		has_hit = False
		for entry_idx, entry in enumerate(entries_to_hit):
			if already_hit[entry_idx] is True:
				continue
			if sql_is_same(qry_formatted['sql'], entry['sql']):
				already_hit[entry_idx] = True
				hit_length += 1
				print("hit!")
				has_hit = True
				turns_since_last_hit = 0
				break
		if not has_hit:
			#print("miss!")
			turns_since_last_hit += 1
			if turns_since_last_hit >= 3000:
				break
		if i % 100 == 0:
			print("step %d: total %d, of which %d were hit!" % (i, total_length, hit_length))

	print("Total: %d, of which %d were hit!" % (total_length, hit_length))
	for idx, item in enumerate(entries_to_hit):
		if already_hit[idx] is False:
			print(item['query_toks_no_value'])
			print("")

	crsr.close()
	conn.close()
	fp.close()


def convert(file_path, mode):
	assert mode in ['all', 'random']
	with open(TABLE_METADATA_PATH, 'r') as fp:
		meta_data = json.load(fp)

	last_dbid = None
	database_path, database_name, typenps, propertynps, type_matrix, property_matrix, prop_rels, fk_rels, valid_database, \
	conn, crsr, num_queries = None, None, None, None, None, None, None, None, None, None, None, None

	with open(file_path, 'r') as fp:
		sql_dcts = json.load(fp)

	skip_list = [6, 425, 426, 437, 438, 521, 522, 629, 630, 631, 632, 635, 636, 639, 640, 660, 661, 921, 922, 947,
				 948, 955, 956, 1038, 1284, 1320, 1416, 1417, 1430, 1431, 1432, 1433, 1434, 1435, 1438, 1439, 1454,
				 1455, 1502, 1687, 1688, 1731, 1802, 1803, 1814, 1815, 1820, 1821, 1832, 1835, 1838, 1928, 2093,
				 2094, 2147, 2175, 2176, 2211, 2212, 2227, 2228, 2229, 2230, 2231, 2232, 2380, 2381, 2382, 2383,
				 2472, 2473, 2478, 2479, 2480, 2481, 2524, 2525, 2651, 2684, 2685, 2724, 2725, 2730, 2731, 2814,
				 2815, 2962, 2963, 2996, 2997, 3068, 3069, 3078, 3079, 3207, 3208, 3251, 3252, 3253, 3254, 3275,
				 3276, 3277, 3278, 3283, 3284, 3285, 3286, 3295, 3296, 3307, 3308, 3321, 3322, 3323, 3324, 3327,
				 3328, 3335, 3336, 3527, 3528, 3655, 3656, 3920, 3921, 3940, 3941, 3942, 3943, 3950, 3951, 3972,
				 3973, 3974, 3975, 3988, 3989, 4108, 4109, 4324, 4325, 4334, 4335, 4336, 4337, 4348, 4349, 4380,
				 4381, 4608, 4609, 4697, 4698, 4763, 4764, 4777, 4778, 4779, 4780, 4785, 4786, 4787, 4788, 4815,
				 4816, 4817, 4818, 5008, 5009, 5146, 5147, 5184, 5185, 5188, 5189, 5190, 5191, 5204, 5205, 5230,
				 5231, 5238, 5239, 5242, 5243, 5256, 5257, 5262, 5263, 5666, 5718, 5719, 5736, 5737, 5740, 5741,
				 5742, 5743, 5744, 5745, 5746, 5747, 5748, 5749, 5752, 5753, 5766, 5767, 5864, 5865, 5931, 5932,
				 6047, 6048, 6276, 6277, 6381, 6485, 6486, 6487, 6488, 6513, 6514, 6614, 6795, 6796, 6871, 6872,
				 6958, 427, 428, 429, 430, 643, 644, 645, 646, 889, 890, 923, 924, 969, 970, 1288, 1792, 1793,
				 1794, 1795, 1840, 2081, 2082, 2171, 2172, 2173, 2174, 2177, 2178, 2179, 2180, 2181, 2182, 2183,
				 2184, 2185, 2186, 2187, 2188, 2199, 2200, 2207, 2208, 2209, 2210, 2219, 2220, 2221, 2222, 2223,
				 2224, 2225, 2226, 2484, 2485, 2836, 2838, 2839, 2848, 2850, 2874, 2875, 2876, 2877, 2878, 2879,
				 2880, 2881, 2882, 2883, 2884, 2885, 2886, 2887, 2888, 2889, 2894, 2895, 2896, 2897, 2908, 2909,
				 2910, 2911, 3125, 3126, 3293, 3294, 3697, 3698, 3725, 3986, 3987, 3990, 3991, 4266, 4267, 4268,
				 4269, 4270, 4271, 4272, 4273, 4274, 4275, 4276, 4277, 4278, 4279, 4300, 4301, 4302, 4303, 4304,
				 4305, 4306, 4307, 4308, 4309, 4310, 4311, 4312, 4313, 4366, 4367, 4523, 4524, 4833, 4834, 4916,
				 4917, 4922, 4923, 4924, 4925, 4928, 4929, 5158, 5159, 5440, 5441, 5562, 5563, 5568, 5569, 5570,
				 5571, 5572, 5573, 5574, 5575, 5576, 5577, 5588, 5589, 5594, 5595, 5598, 5599, 5702, 5703, 5704,
				 5705, 5760, 5761, 5778, 5779, 5794, 5795, 5959, 5960, 6079, 6080, 6081, 6082, 6085, 6086, 6107,
				 6108, 6109, 6110, 6111, 6112, 6137, 6138, 6141, 6142, 6619, 6797, 6798, 6799, 6800, 6955]

	# collected from 'T3'
	hidden_faulty_list = [13, 81, 82, 85, 86, 93, 94, 423, 424, 658, 659, 1037, 1282, 1283, 1289, 1317, 1318,
						  1319, 1386, 1387, 1450, 1451, 1456, 1457, 1460, 1461, 1501, 1503, 1621, 1622, 1640,
						  1646, 1728, 1729, 1730, 1790, 1791, 1798, 1799, 1800, 1801, 1810, 1811, 1816, 1817,
						  1818, 1819, 1822, 1823, 1839, 1843, 1906, 1953, 1966, 1987, 1988, 1989, 2027, 2028,
						  2029, 2030, 2037, 2038, 2092, 2251, 2252, 2253, 2254, 2255, 2256, 2476, 2477, 2678,
						  2679, 2680, 2681, 2682, 2683, 2726, 2727, 2732, 2733, 2812, 2813, 2968, 2969, 3129,
						  3130, 3133, 3134, 3143, 3146, 3153, 3177, 3178, 3239, 3240, 3243, 3244, 3245, 3246,
						  3279, 3280, 3311, 3312, 3313, 3314, 3315, 3316, 3317, 3318, 3461, 3462, 3517, 3518,
						  3519, 3520, 3523, 3524, 3724, 3852, 3853, 3914, 3915, 3922, 3923, 3976, 3977, 3978,
						  3979, 3980, 3981, 3982, 3983, 3984, 3985, 4106, 4107, 4220, 4221, 4234, 4235, 4340,
						  4341, 4362, 4363, 4482, 4483, 4561, 4562, 4563, 4564, 4565, 4566, 4607, 4610, 4611,
						  4612, 4819, 4820, 5068, 5069, 5182, 5183, 5186, 5187, 5196, 5197, 5240, 5241, 5258,
						  5259, 5266, 5267, 5268, 5269, 5270, 5271, 5388, 5389, 5390, 5391, 5420, 5421, 5664,
						  5750, 5751, 5756, 5757, 5758, 5759, 5762, 5763, 5764, 5765, 6290, 6291, 6292, 6293,
						  6294, 6295, 6359, 6491, 6492, 6501, 6502, 6511, 6512, 6515, 6516, 6613, 6620, 6777,
						  6778, 6996, 6997]

	# collected from 't3'
	hidden_faulty_list += [2864, 2898, 911, 912, 913, 914, 1519, 1520, 1521, 1522, 2865, 2866, 2867, 2868, 2869,
							2870, 2871, 2872, 2873, 2890, 2891, 2892, 2893, 2899, 2900, 2901, 2902, 2903, 3167,
							3404, 4258, 4259, 4260, 4261, 4262, 4263, 4264, 4265, 4294, 4295, 4296, 4297, 4314,
							4315, 4316, 4317, 4318, 4319, 4525, 4526, 4920, 4921, 4942, 4943, 5548, 5549, 5556,
							5557, 5558, 5559, 5560, 5561, 5564, 5565, 5566, 5567, 5628, 5629, 6077, 6078, 6083,
							6084, 6113, 6114, 6115, 6116, 6322, 6323, 6324, 6325]
	'''
	if len(sql_dcts) != 7000:
		print(len(sql_dcts))
		raise AssertionError
	not_skip_list = []
	for i in range(7000):
		if i not in skip_list and i not in hidden_faulty_list:
			not_skip_list.append(i)
	'''

	corrected_list = [5263, 6, 5569, 6614, 4524, 6110, 5230, 6110, 5146, 1417, 5742, 4304, 2209, 521, 1793, 4764,
					  2176, 2383, 4697, 1417, 437, 5240, 3317, 3315, 2038, 1037, 5183, 423, 5259, 3243, 1522, 2865,
					  4258, 2873]
	#to_convert_list = corrected_list + numpy.random.choice(not_skip_list, size=(353-len(corrected_list))).tolist()
	to_convert_list = [5263, 6, 5569, 6614, 4524, 6110, 5230, 6110, 5146, 1417, 5742, 4304, 2209, 521, 1793, 4764,
					   2176, 2383, 4697, 1417, 437, 5240, 3317, 3315, 2038, 1037, 5183, 423, 5259, 3243, 1522, 2865,
					   4258, 2873, 2404, 4655, 1001, 5178, 2748, 5982, 1579, 4062, 384, 2459, 1914, 293, 3455, 6101,
					   2077, 3371, 4283, 709, 3458, 1749, 2400, 1405, 5289, 1325, 2518, 3919, 753, 5872, 6936, 234,
					   5413, 6489, 4974, 4013, 2377, 2536, 2271, 6223, 5029, 6231, 692, 3752, 2643, 2453, 711, 4559,
					   615, 1744, 3786, 5362, 74, 3810, 6415, 1876, 2052, 3175, 2809, 557, 4580, 3813, 6606, 5096,
					   4559, 1075, 4999, 3502, 341, 5416, 1994, 4041, 6754, 4438, 3005, 2952, 5551, 4463, 3299, 1125,
					   6075, 1399, 2303, 38, 4510, 195, 2990, 48, 5656, 5755, 2613, 5234, 3777, 2101, 1970, 6862,
					   1364, 5137, 2329, 324, 5593, 1554, 1900, 1152, 2430, 1516, 2662, 4798, 5383, 311, 3667, 6735,
					   87, 5543, 4069, 1709, 5798, 4885, 1400, 6589, 6545, 6193, 1898, 2262, 387, 5198, 2246, 2608,
					   2233, 3423, 5217, 3686, 5997, 3258, 4686, 5710, 6412, 2579, 387, 1739, 744, 6350, 4406, 5553,
					   4591, 824, 5414, 6220, 3396, 6039, 6976, 1330, 4356, 1073, 4718, 869, 2101, 5827, 6091,
					   3341, 5634, 2261, 4410, 6384, 1250, 4537, 4559, 1672, 6383, 3479, 5770, 3564, 5098, 6576, 6792,
					   2215, 5806, 6990, 6307, 883, 2623, 6967, 217, 6841, 4150, 741, 2088, 4700, 3067, 3801, 6538,
					   1713, 187, 3234, 3607, 2737, 329, 959, 2829, 2718, 6401, 3466, 1335, 497, 1595, 1425, 474,
					   5945, 494, 1249, 3149, 4668, 4224, 3452, 6045, 432, 5428, 1921, 64, 865, 6248, 5510, 1050,
					   5342, 1014, 1617, 3573, 5542, 928, 4987, 6663, 857, 858, 6916, 1871, 5143, 4084, 6312, 5133,
					   5508, 2344, 5483, 6232, 1339, 767, 1873, 2858, 2938, 233, 4672, 1303, 3522, 5903, 4912, 6570,
					   4683, 219, 60, 6356, 2926, 2036, 5454, 375, 2787, 546, 3623, 4142, 1616, 2522, 2783, 3749,
					   5289, 1639, 6153, 3860, 1583, 1656, 2158, 6747, 2648, 449, 3563, 1121, 5790, 2762, 4440,
					   5219, 4538, 580, 5898, 1258, 2860, 6803, 2305, 6156, 3567, 4101, 1373, 2064, 154, 3574, 2647,
					   6268, 1209, 5299, 6887, 3652, 2305, 389, 308, 6586, 4706, 3152, 2036, 1114, 6426, 5680, 4568,
					   309, 5710]

	print('to_convert_list: ')
	print(to_convert_list)
	unexpressable_cnt = 0
	unexpressable_entries = []
	error_bucket = {}
	res_json = []
	for _, dct in enumerate(sql_dcts[args.start_from:]):
		dct_idx = _ + args.start_from
		if dct_idx % 100 == 1:
			print("turn %d begins!" % dct_idx)
		if mode == 'random' and dct_idx not in to_convert_list:
			continue

		if dct['db_id'] != last_dbid:
			last_dbid = dct['db_id']
			db_num = None
			for db_idx, item in enumerate(meta_data):
				if item['db_id'] == dct['db_id']:
					db_num = db_idx
					meta = item
					break
			assert db_num is not None
			database_path, database_name, typenps, propertynps, type_matrix, property_matrix, prop_rels, fk_rels, \
			valid_database, conn, crsr, num_queries = build_spider_dataset(db_num)
		if dct['db_id'] == 'formula_1':
			continue
		entry_sql = dct['sql']
		#pack = np_from_entry(entry_sql=entry_sql, typenps=typenps, propertynps=propertynps)

		if 't3' in dct['query'].lower() and len(dct['sql']['from']['table_units']) <= 2 and dct_idx not in skip_list and dct_idx not in hidden_faulty_list:
			print(dct_idx)
			print(dct['query'])
			print(dct['sql']['from'])
			hidden_faulty_list.append(dct_idx)
		#try:
		pack = np_from_entry(entry_sql=entry_sql, typenps=typenps, propertynps=propertynps, fk_rels=fk_rels,
								 finalize=True)
		'''
		except Exception as e:
			print("False gold SQL: %d" % dct_idx)
			print(dct['query'])
			print(entry_sql['from']['table_units'])
			if dct['db_id'] == 'formula_1':
				continue
			if dct_idx not in skip_list:
				skip_list.append(dct_idx)
				continue
			#elif dct_idx not in corrected_list:
			#	continue
			else:
				continue
			#	raise
		'''
		if (dct_idx in skip_list or dct_idx in hidden_faulty_list) and dct_idx not in corrected_list and mode == 'random':
			print("!")
			raise AssertionError
		if isinstance(pack, str):
			print("Unexpressable_entry_occurred!")
			print(dct['query'])
			unexpressable_entries.append([pack, dct['query']])
			print("")
			if pack not in error_bucket:
				error_bucket[pack] = 1
			else:
				error_bucket[pack] += 1
			unexpressable_cnt += 1
			continue
		#elif isinstance(pack, None):
		#	continue
		else:
			np, qrynp = pack
		for line in qrynp.c_english_sequence:
			print(line)
		print("Gold: "+dct['question'])
		print("")
		print(dct['query'])
		print(qrynp.z)
		print("")

		try:
			qry_returned = crsr.execute(qrynp.z).fetchall()
		except Exception as e:
			if dct['db_id'] == 'formula_1':
				continue
			print(e)
			continue

		if len(qry_returned) == 0:
			sample_results = []
		else:
			sample_results = [random.choice(qry_returned)]
		headers = [tup[0] for tup in crsr.description]
		qry_formatted = format_query_to_spider(np, qrynp, database_name, sample=sample_results, headers=headers)
		qry_formatted['question_gold'] = dct['question']
		qry_formatted['global_idx'] = dct_idx
		#res = {'sql': dct['query'], 'query_toks': dct['query_toks'], 'query_toks_no_value': dct['query_toks_no_value'],
		#	   'gold_question': dct['question'], 'canonical_ce': qrynp.c_english_verbose,
		#	   'canonical_ce_sequence': qrynp.c_english_sequence}
		#res_json.append(res)
		res_json.append(qry_formatted)
	print(len(skip_list))
	print("hidden faulty list: ")
	print(hidden_faulty_list)
	print("unexpressable_cnt: ", unexpressable_cnt)
	print("error bucket: ")
	for item in error_bucket:
		print(item, ': ', error_bucket[item])
	print("")
	with open('SPIDER_canonicals_%s.json' % mode, 'w') as fp:
		json.dump(res_json, fp, indent=4)
	with open('SPIDER_unexpressables_%s.json' % mode, 'w') as fp:
		json.dump(unexpressable_entries, fp, indent=4)
	print("convertion finished!")


def test_edge():
	for num in range(166):
		if num in db_ids_to_ignore:
			continue
		database_path, database_name, typenps, propertynps, type_matrix, property_matrix, prop_rels, fk_rels, \
		valid_database, conn, crsr, num_queries = build_spider_dataset(num)


if __name__ == '__main__':
	begin = time.time()
	idx = args.db_id
	if args.mode == 'run':
		main(idx, args.verbose)
	elif args.mode == 'debug':
		debug(True)
	elif args.mode == 'hit':
		hit(idx, 10000, False)
	elif args.mode == 'convert-all':
		convert(args.gold_path, mode='all')
	elif args.mode == 'convert-random':
		convert(args.gold_path, mode='random')
	elif args.mode == 'test_edge':
		test_edge()
	else:
		print("Assigned mode does not match with any programs!")
		raise AssertionError
	end = time.time()

	# display the time spent in this run of query-generation
	dur = end - begin
	string = 'Time spent: '
	if dur > 3600:
		string += '%d hour' % int(dur / 3600)
		if dur > 7200:
			string += 's, '
		else:
			string += ', '
	dur = dur % 3600
	if dur > 60:
		string += '%d minute' % int(dur / 60)
		if dur > 120:
			string += 's, '
		else:
			string += ', '
	dur = dur % 60
	string += '%d seconds' % int(dur)
	print(string)
	print("")
