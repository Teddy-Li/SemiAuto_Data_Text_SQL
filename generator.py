
#-*- coding: utf-8 -*-


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
parser.add_argument('-l', '--split', type=str, default='train')
parser.add_argument('-r', '--ref_json', type=str, default='SPIDER_canonicals_all_train.json')
parser.add_argument('-o', '--out_dir', type=str, default='./spider_converted', help='output directory for "convert"')
parser.add_argument('--lang', type=str, default='eng')
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


def generate_queries(database_idx, verbose, ref_json):
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
		qry_vecs = sql_features(qry_formatted['sql'])
		qry_formatted['sql_vec'] = qry_vecs
		qry_formatted = fetch_refs([qry_formatted], ref_json)[0]


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


def main(idx, verbose, ref_dir):
	# generate queries for English
	if idx in db_ids_to_ignore:
		print("database number %d ignored!" % idx)
	print("began generating query entries for database $ {0} $".format(idx))

	with open(ref_dir, 'r') as fp:
		ref_json = json.load(fp)

	res_saved, res_dumped, gold_saved, gold_dumped, canonical_saved, canonical_dumped = generate_queries(idx, verbose, ref_json)
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


def debug(db_idx, verbose):
	database_path, database_name, tnps, pnps, type_m, property_m, prop_r, fk_rels, valid, conn, crsr, num_queries = build_spider_dataset(
		db_idx)
	assert valid
	fp = open('./result_0131.jsonl', 'w')
	for i in range(ITR_TRY):
		np, qrynp = scratch_build(tnps, pnps, type_m, property_m, prop_r, fk_rels, finalize_sequence=True, cursor=crsr,
								  print_verbose=verbose)

		print('SQL: ', qrynp.z)
		print("Full question English: ", qrynp.c_english)
		print("Full question Chinese: ", qrynp.c_chinese)
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


def convert(file_path, mode, set_split, lang):
	assert mode in ['all', 'random']
	with open(TABLE_METADATA_PATH, 'r') as fp:
		meta_data = json.load(fp)

	last_dbid = None
	database_path, database_name, typenps, propertynps, type_matrix, property_matrix, prop_rels, fk_rels, valid_database, \
	conn, crsr, num_queries = None, None, None, None, None, None, None, None, None, None, None, None

	with open(file_path, 'r') as fp:
		sql_dcts = json.load(fp)

	unexpressable_cnt = 0
	unexpressable_entries = []
	error_bucket = {}
	res_json = []
	proposer_json = {'src': [], 'tgt': []}
	proposer_tsv = []
	pseudo_tsv = []
	simple_tsv = []
	for _, dct in enumerate(sql_dcts[args.start_from:]):
		dct_idx = _ + args.start_from
		if dct_idx % 100 == 1:
			print("turn %d begins!" % dct_idx)
		if dct['db_id'] == 'wta_1':
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

		'''
		if 't3' in dct['query'].lower() and len(dct['sql']['from']['table_units']) <= 2:
			print("----------------------")
			print(dct['query'])
			print(dct_idx)
			raise AssertionError
		'''
		try:
			pack = np_from_entry(entry_sql=entry_sql, typenps=typenps, propertynps=propertynps, fk_rels=fk_rels,
								 finalize=True)
		except Exception as e:
			print("False gold SQL: %d" % dct_idx)
			print(dct['query'])
			print(entry_sql['from']['table_units'])
			continue

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

		# for line in qrynp.c_english_sequence:
		# 	print(line)
		for line in qrynp.c_chinese_sequence:
			print(line)
		print("Gold: "+dct['question'])
		print("")
		print(dct['query'])
		print(qrynp.z)
		print("")

		# if len(qrynp.c_chinese_sequence) > 2:
		# 	print("!")

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

		qry_vecs = sql_features(qry_formatted['sql'])
		qry_formatted['sql_vec'] = qry_vecs

		#res = {'sql': dct['query'], 'query_toks': dct['query_toks'], 'query_toks_no_value': dct['query_toks_no_value'],
		#	   'gold_question': dct['question'], 'canonical_ce': qrynp.c_english_verbose,
		#	   'canonical_ce_sequence': qrynp.c_english_sequence}
		#res_json.append(res)
		res_json.append(qry_formatted)

		if lang == 'eng':
			src = copy.deepcopy(qry_formatted['question_sequence'])
		elif lang == 'chi':
			src = copy.deepcopy(qry_formatted['question_sequence_chinese'])
		else:
			raise AssertionError
		# wash out the '$' and '#' marks
		for src_i in range(len(src)):
			src[src_i] = src[src_i].replace('@', '').replace('$', '').replace('\"', '')[10:]
		proposer_json['src'].append('. '.join(src))
		proposer_json['tgt'].append(copy.deepcopy(dct['question']))
		proposer_tsv.append('\"'+'. '.join(src)+'\"\t\"'+copy.deepcopy(dct['question'])+'\"')
		pseudo_tsv.append('\"'+'. '.join(src)+'\"\t\"'+'. '.join(src)+'\"')
		if is_simple_query(qrynp):
			simple_tsv.append('\"'+'. '.join(src)+'\"\t\"'+copy.deepcopy(dct['question'])+'\"')
		'''
		with open('../pre-paraphrasers/data/cnnstyle/%s/%s_%d.story' % (set_split, set_split, dct_idx), 'w') as fp:
			for line in src:
				fp.write(line+'\n\n')
			fp.write('@highlight\n')
			fp.write(dct['question'])
		'''

	if set_split == 'train':
		res_json = fetch_refs(res_json, res_json, lang=lang, same=True)
	elif set_split == 'val':
		with open(args.ref_json, 'r') as fp:
			ref_json = json.load(fp)
		res_json = fetch_refs(res_json, ref_json, lang=lang)
	else:
		raise AssertionError

	print("len tsv: ", len(proposer_tsv))
	print("len simple tsv: ", len(simple_tsv))
	proposer_tsv = '\n'.join(proposer_tsv)
	pseudo_tsv = '\n'.join(pseudo_tsv)
	simple_tsv = '\n'.join(simple_tsv)
	'''
	with open('../pre-paraphrasers/data/tsv/%s.tsv' % set_split, 'w') as fp:
		fp.write(proposer_tsv)
	with open('../pre-paraphrasers/data/pseudo_tsv/%s.tsv' % set_split, 'w') as fp:
		fp.write(pseudo_tsv)
	with open('../pre-paraphrasers/data/simple_tsv/%s.tsv' % set_split, 'w') as fp:
		fp.write(simple_tsv)
	with open('../pre-paraphrasers/data/json/%s.json' % set_split, 'w') as fp:
		json.dump(proposer_json, fp)
	'''
	print("unexpressable_cnt: ", unexpressable_cnt)
	print("error bucket: ")
	for item in error_bucket:
		print(item, ': ', error_bucket[item])
	print("")
	assert len(proposer_json['src']) == len(proposer_json['tgt'])

	with open(os.path.join(args.out_dir, 'canonicals_%s_%s.json'%(mode, set_split)), 'w', encoding='utf-8') as fp:
		json.dump(res_json, fp, ensure_ascii=False, indent=4)
	with open(os.path.join(args.out_dir, 'unexpressables_%s_%s.json'%(mode, set_split)), 'w', encoding='utf-8') as fp:
		json.dump(unexpressable_entries, fp, ensure_ascii=False, indent=4)
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
		main(idx, args.verbose, args.ref_json)
	elif args.mode == 'debug':
		debug(idx, True)
	elif args.mode == 'hit':
		hit(idx, 10000, False)
	elif args.mode == 'convert-all':
		convert(args.gold_path, mode='all', set_split=args.split, lang=args.lang)
	elif args.mode == 'convert-random':
		convert(args.gold_path, mode='random', set_split=args.split, leng=args.lang)
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
