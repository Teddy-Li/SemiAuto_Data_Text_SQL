from phrase_structures import *
from utils import *
import json
import sqlite3


def build_spider_dataset(num):
	valid = True  # if there exists empty columns in the database, then the database is assumed invalid, reported
	# and discarded
	with open(TABLE_METADATA_PATH, 'r') as fp:
		json_file = json.load(fp)
	meta = json_file[num]
	if SETTING in ['spider', 'chisp']:
		database_name = DATABASE_PATH + '/%s/%s.sqlite' % (meta['db_id'], meta['db_id'])
	elif SETTING == 'dusql':
		database_name = DATABASE_PATH + '/%s.db' % meta['db_id']
	else:
		raise AssertionError
	print(meta['db_id'])
	propertynps = []

	conn = sqlite3.connect(database_name)
	# conn.create_function('FIXENCODING', 1, lambda s: str(s).encode('latin_1'))
	crsr = conn.cursor()

	typenps = []
	print("")
	print("All table names: ")
	for idx, tab_name in enumerate(meta['table_names']):
		c_english = '@' + tab_name + '@'
		c_chinese = '@' + tab_name + '@'
		z = meta['table_names_original'][idx]
		print(z)
		if ' ' in z or z[0] in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'] or '(' in z:
			z = '[' + z + ']'
		properties = []
		for prop_idx, prop_ori in enumerate(meta['column_names']):
			if prop_ori[0] == idx:
				# '*' disgarded at generation time
				properties.append(prop_idx - 1)
		typenps.append(TYPENP(c_english=c_english, c_chinese=c_chinese, z=z, overall_idx=idx, properties=properties))

	# disgard the '*'
	for idx, prop_name in enumerate(meta['column_names']):
		table_idx = prop_name[0]
		if table_idx < 0:
			assert idx == 0
			# print('star: ', idx, prop_name)
			continue
		c_english = '{0}$' + prop_name[1] + '$'
		c_chinese = '{0}$' + prop_name[1] + '$'
		col_z = meta['column_names_original'][idx][1]
		# if the name contains ' ', wrap it with brackets
		if ' ' in col_z or col_z[0] in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'] or '(' in col_z:
			col_z = '[' + col_z + ']'
		z = '{0}%s' % col_z
		dtype = meta['column_types'][idx]
		if idx in meta['primary_keys']:
			is_primary_key = True
		else:
			is_primary_key = False
		cur_prop = PROPERTYNP(c_english=c_english, c_chinese=c_chinese, z=z, dtype=dtype, table_id=table_idx,
					   overall_idx=idx - 1, meta_idx=idx - 1, is_pk=is_primary_key)
		if idx in meta['primary_keys']:
			cur_prop.is_primary = True
		'''
		query_all = 'select %s from %s' % (cur_prop.z.format(''), typenps[cur_prop.table_id].z)
		try:
			all_values = crsr.execute(query_all).fetchall()
		except Exception as e:
			print(e)
			print(query_all)
			raise
		temp_all_values = []
		for item in all_values:
			try:
				item_str = str(item)
				temp_all_values.append(item_str)
			except Exception as e:
				continue
		if is_unique(temp_all_values):
			cur_prop.is_unique = True
		else:
			cur_prop.is_unique = False
		'''
		propertynps.append(cur_prop)

	fks = meta['foreign_keys']  # a.k.a the prop_rels

	#typenp_unique_fk_num = [[] for _ in range(len(typenps))]

	for pair in fks:
		assert pair[0]-1 >= 0 and pair[1]-1 >= 0
		propertynps[pair[0]-1].is_fk_left = True
		propertynps[pair[1]-1].is_fk_right = True
		'''
		prop1 = propertynps[pair[0]-1]
		prop2 = propertynps[pair[1]-1]
		tid1 = prop1.table_id
		tid2 = prop2.table_id
		# if the other side of this foreign key relation is a primary key, it means the column in this side of this
		# foreign key relation is a reference, namely an edge column, two edge columns make up an edge table
		if prop1.is_primary and (pair[1]-1) not in typenp_unique_fk_num[tid2]:
			typenp_unique_fk_num[tid2].append(pair[1]-1)
		if prop2.is_primary and (pair[0]-1) not in typenp_unique_fk_num[tid1]:
			typenp_unique_fk_num[tid1].append(pair[0]-1)
		'''
	#for tid, item in enumerate(typenp_unique_fk_num):
	#	if len(item) == 2:
	#		typenps[tid].is_edge = True
	#		print("edge table: %s" % typenps[tid].c_english)
	#print("")
	for pair in fks:
		if meta['column_names'][pair[0]][0] == meta['column_names'][pair[1]][0]:
			new_typenp = copy.deepcopy(typenps[meta['column_names'][pair[0]][0]])
			new_typenp.overall_idx = len(typenps)
			new_typenp.properties = []
			for pid in typenps[meta['column_names'][pair[0]][0]].properties:
				new_prop = copy.deepcopy(propertynps[pid])
				new_prop.table_id = new_typenp.overall_idx
				new_prop.overall_idx = len(propertynps)
				new_typenp.properties.append(new_prop.overall_idx)
				propertynps.append(new_prop)
				propertynps[pid].clones.append(new_prop.overall_idx)
			typenps.append(new_typenp)
			typenps[meta['column_names'][pair[0]][0]].clones.append(new_typenp.overall_idx)

	# here they're both directed
	fk_type_matrix = [[0] * len(typenps) for i in range(len(typenps))]
	fk_property_matrix = [[0] * len(propertynps) for i in range(len(propertynps))]

	for pair in fks:
		if meta['column_names'][pair[0]][0] == meta['column_names'][pair[1]][0]:
			tid = meta['column_names'][pair[0]][0]
			for i, clone_id in enumerate(typenps[tid].clones):
				fk_type_matrix[clone_id][tid] += 1
				fk_property_matrix[pair[0] - 1][propertynps[pair[1] - 1].clones[i]] += 1
				fk_property_matrix[propertynps[pair[0] - 1].clones[i]][pair[1] - 1] += 1
		else:
			fk_property_matrix[pair[0] - 1][pair[1] - 1] += 1
			fk_type_matrix[meta['column_names'][pair[1]][0]][meta['column_names'][pair[0]][0]] += 1
	# PROBABLY BRING THE NAME MATCHING INTO ACCOUNT?
	nm_type_matrix = [[0] * len(typenps) for i in range(len(typenps))]
	nm_property_matrix = [[0] * len(propertynps) for i in range(len(propertynps))]
	for i in range(len(propertynps)):
		for j in range(len(propertynps)):
			if propertynps[i].table_id in typenps[propertynps[j].table_id].clones or propertynps[j].table_id in typenps[
				propertynps[i].table_id].clones:
				continue
			if propertynps[i].z.format('') == propertynps[j].z.format(''):
				if propertynps[i].z.format('').lower() == 'id' and propertynps[i].overall_idx != propertynps[
					j].overall_idx \
						and propertynps[i].overall_idx not in propertynps[j].clones \
						and propertynps[j].overall_idx not in propertynps[i].clones:
					continue
				nm_property_matrix[i][j] += 1 * NAME_PAIR_WEIGHT
				nm_property_matrix[j][i] += 1 * NAME_PAIR_WEIGHT
				nm_type_matrix[propertynps[j].table_id][propertynps[i].table_id] += 1 * NAME_PAIR_WEIGHT
				nm_type_matrix[propertynps[i].table_id][propertynps[j].table_id] += 1 * NAME_PAIR_WEIGHT

	for prop in propertynps:
		# if prop.dtype == 'varchar(1000)':
		#	conn.execute('UPDATE "{0}" SET "{1}" = FIXENCODING("{1}")'.format(typenps[prop.table_id].z, prop.z.format('')))
		query_all = 'select %s from %s' % (prop.z.format(''), typenps[prop.table_id].z)
		try:
			all_values = crsr.execute(query_all).fetchall()
		except Exception as e:
			print(e)
			print(query_all)
			raise
		# if a property is empty before discarding invalid values, it means whole tables have no entries, assume as
		# invalid database
		if len(all_values) == 0:
			print("Empty property: ", prop.z.format(typenps[prop.table_id].z))
			valid = False
		if len(all_values) > MAX_VALUE_ENTRIES:
			all_values = random.choices(all_values, k=MAX_VALUE_ENTRIES)

		temp_all_values = []
		for idx, item in enumerate(all_values):
			if isinstance(item, tuple) or isinstance(item, list):
				assert (len(item) == 1)
				item = item[0]
			# if isinstance(item, bytes):
			#	item = item.decode('latin_1')
			try:
				item_str = str(item)
			except Exception as e:
				print("!")
				item_str = item
			if prop.dtype in ['datetime', 'bool'] and item_str[0] not in ["'", '"']:
				item_str = "'" + item_str + "'"
			elif ('varchar' in prop.dtype or prop.dtype in ['bit', 'id']) and len(item_str) > 0 and "'" != item_str[0] and '"' != item_str[0] and "'" != item_str[-1] and '"' != item_str[-1]:
				item_str = '"' + item_str + '"'
			elif SETTING == 'dusql':
				item_str = '"' + item_str.strip('\"\'') + '"'

			# do not save those None or '' values to avoid confusion
			if item is None or item == '':
				continue
			temp_all_values.append(
				VALUENP(c_english=item_str, c_chinese=item_str, z=item_str, dtype=prop.dtype))

		# if prop.dtype == 'bool':
		#	print(all_values)
		all_values = temp_all_values
		if is_unique(all_values):
			prop.is_unique = True

		prop.set_values(all_values)

	for idx in range(len(typenps)):
		type_valid = False
		for pid in typenps[idx].properties:
			if propertynps[pid].valid is True:
				type_valid = True
		if not type_valid:
			typenps[idx].valid = False

	type_matrix = [[0] * len(typenps) for i in range(len(typenps))]
	for i in range(len(type_matrix)):
		for j in range(len(type_matrix[0])):
			type_matrix[i][j] = fk_type_matrix[i][j] + nm_type_matrix[i][j]

	property_matrix = [[0] * len(propertynps) for i in range(len(propertynps))]
	for i in range(len(property_matrix)):
		for j in range(len(property_matrix)):
			property_matrix[i][j] = fk_property_matrix[i][j] + nm_property_matrix[i][j]

	property_matrix_np = numpy.matrix(property_matrix)
	# don't tune property edge weight with pagerank, do it through type pagerank.
	'''
	property_scores = pagerank_scores(property_matrix_np)
	# print(property_matrix)
	# print("")
	# print(property_scores)
	for i in range(len(property_matrix)):
		coefficient = property_scores[i] / float(sum(property_matrix[i]))
		for j in range(len(property_matrix)):
			property_matrix[i][j] = coefficient * property_matrix[i][j]
	'''

	# turn the property_matrix un-directioned
	#for i in range(len(property_matrix)):
	#	for j in range(len(property_matrix)):
	#		property_matrix[i][j] += property_matrix[j][i]
	fk_prop_rels = []
	for pair in fks:
		if propertynps[pair[0] - 1].table_id == propertynps[pair[1] - 1].table_id:
			tid = propertynps[pair[0] - 1].table_id
			for i, clone_id in enumerate(typenps[tid].clones):
				fk_prop_rels.append(
					PROP_REL(pair[0] - 1, propertynps[pair[1] - 1].clones[i], tid,
							 clone_id,
							 property_matrix[pair[0] - 1][propertynps[pair[1] - 1].clones[i]]))
				fk_prop_rels.append(
					PROP_REL(propertynps[pair[0] - 1].clones[i], pair[1] - 1, clone_id,
							 tid,
							 property_matrix[propertynps[pair[0] - 1].clones[i]][pair[1] - 1]))
		fk_prop_rels.append(
			PROP_REL(pair[0] - 1, pair[1] - 1, propertynps[pair[0] - 1].table_id, propertynps[pair[1] - 1].table_id,
					 property_matrix[pair[0] - 1][pair[1] - 1]))

	nm_prop_rels = []
	for prop1 in propertynps:
		for prop2 in propertynps:
			if prop1.table_id in typenps[prop2.table_id].clones or prop2.table_id in typenps[
				prop1.table_id].clones or prop1.overall_idx == prop2.overall_idx:
				continue
			if prop1.z == prop2.z:
				# exclude those 'id' queries which are not in the same query
				if prop1.z.format('').lower() == 'id' and prop1.overall_idx != prop2.overall_idx \
						and prop1.overall_idx not in prop2.clones and prop2.overall_idx not in prop1.clones:
					continue
				nm_prop_rels.append(PROP_REL(prop1.overall_idx, prop2.overall_idx, prop1.table_id, prop2.table_id,
											 property_matrix[prop1.overall_idx][prop2.overall_idx] * NAME_PAIR_WEIGHT))
	prop_rels = fk_prop_rels + nm_prop_rels
	type_matrix = numpy.matrix(type_matrix)
	property_matrix = numpy.matrix(property_matrix)

	for p in propertynps:
		if p.values is None:
			print(p.z)
			raise AssertionError
		if len(p.values) == 0:
			p.valid = False
	print("Finished resolving database $ %s $" % meta['db_id'])

	num_queries = calc_num_queries_via_stats(len(fk_prop_rels))
	return database_name, meta[
		'db_id'], typenps, propertynps, type_matrix, property_matrix, prop_rels, fk_prop_rels, valid, conn, crsr, num_queries