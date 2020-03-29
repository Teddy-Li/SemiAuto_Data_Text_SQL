import numpy as np
import json
import copy
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-m', '--mode', type=str, default='skip')
parser.add_argument('-f', '--input_filename', type=str, default='train_spider.json')


def get_from_info(table_dct, meta):
	table_ready = False
	while not table_ready:
		table_info = []
		tids_raw = []
		t_name_string = input('table names?    ')
		t_name_string = [item.strip() for item in t_name_string.split(';')]
		faulty = False
		for t_name in t_name_string:
			try:
				t_idx = table_dct[t_name.lower()]
			except Exception as e:
				print("unresolved table name!")
				faulty = True
				break
			table_info.append(['table_unit', t_idx])
			tids_raw.append(t_idx)
		if not faulty:
			table_ready = True

	cond_info = []
	cond_ready = False
	while not cond_ready:
		cond_string = input('cond column names?    ')
		if len(cond_string) == 0:
			cond_ready = True
			break
		cond_cols = [item.strip() for item in cond_string.split('=')]
		cond_colidx_pair = []
		faulty = False
		for item in cond_cols:
			item = item.split('.')
			t_order = int(item[0][1:])-1
			t_idx = tids_raw[t_order]
			col_idx = None
			for _, col in enumerate(meta['column_names_original']):
				if col[1].lower() == item[1].lower() and t_idx == col[0]:
					col_idx = _
					break
			if col_idx is None:
				print("columns not found!")
				faulty = True
				break
			cond_colidx_pair.append(col_idx)
		if faulty:
			continue
		cond_info.append(
			[False, 2, [0, [0, cond_colidx_pair[0], False], None], [0, cond_colidx_pair[1], False], None])
		cond_info.append('and')
	cond_info = cond_info[:-1]

	sql_from = {'conds': cond_info, 'table_units': table_info}
	print("re-annotated sql_from info: ")
	print(sql_from)
	return sql_from


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

hidden_faulty_list = [2864, 2898, 911, 912, 913, 914, 1519, 1520, 1521, 1522, 2865, 2866, 2867, 2868, 2869,
							2870, 2871, 2872, 2873, 2890, 2891, 2892, 2893, 2899, 2900, 2901, 2902, 2903, 3167,
							3404, 4258, 4259, 4260, 4261, 4262, 4263, 4264, 4265, 4294, 4295, 4296, 4297, 4314,
							4315, 4316, 4317, 4318, 4319, 4525, 4526, 4920, 4921, 4942, 4943, 5548, 5549, 5556,
							5557, 5558, 5559, 5560, 5561, 5564, 5565, 5566, 5567, 5628, 5629, 6077, 6078, 6083,
							6084, 6113, 6114, 6115, 6116, 6322, 6323, 6324, 6325]


def main(args):
	percentage = float(input('percentage point of entries to choose')) / 100

	if args.mode == 'skip':
		size = int(len(skip_list) * percentage)+1
		chosen = np.random.choice(skip_list, size=size)
	elif args.mode == 'hidden':
		size = int(len(hidden_faulty_list) * percentage) + 1
		chosen = np.random.choice(hidden_faulty_list, size=size)
	elif args.mode == 'assigned':
		chosen = np.array([5263, 6, 5569, 6614, 4524, 6110, 5230, 6110, 5146, 1417, 5742, 4304, 2209, 521, 1793, 4764,
					  2176, 2383, 4697, 1417, 437, 5240, 3317, 3315, 2038, 1037, 5183, 423, 5259, 3243])
	else:
		raise AssertionError

	print(chosen)
	print("chosen size: ", len(chosen))
	print("")

	with open('./spider/spider/%s' % args.input_filename, 'r') as fp:
		train_data = json.load(fp)

	with open('./spider/spider/tables_mod.json', 'r') as fp:
		table_meta = json.load(fp)

	res = []

	for entry_idx, entry in enumerate(train_data):
		ori_entry = copy.deepcopy(entry)
		if entry_idx not in chosen:
			res.append(entry)
			continue
		print("")
		print(entry_idx)
		meta = None
		for item in table_meta:
			if item['db_id'] == entry['db_id']:
				meta = item
				break
		assert meta is not None

		table_dct = {}
		for item_idx, item in enumerate(meta['table_names_original']):
			item = item.lower()
			assert item not in table_dct
			table_dct[item] = item_idx

		print("query: ", entry['query'])
		print("")
		print("conds: ")
		print(entry['sql']['from']['conds'])
		print("table_units: ")
		prev_tablist = [meta['table_names_original'][item[1]] for item in entry['sql']['from']['table_units']]
		print(entry['sql']['from']['table_units'])
		print(prev_tablist)
		print("")

		skip = input('Skip?        ')
		if skip == 'yes':
			print("entry number %d skipped!" % entry_idx)
			res.append(entry)
			continue
		check = False
		while not check:
			entry = copy.deepcopy(ori_entry)
			sql_from = get_from_info(table_dct, meta)
			entry['sql']['from'] = sql_from
			has_intersect = False
			has_union = False
			has_except = False
			post = None
			if entry['sql']['intersect'] is not None:
				print("detected intersect clause!")
				has_intersect = True
				post = entry['sql']['intersect']
			elif entry['sql']['union'] is not None:
				print("detected union clause!")
				has_union = True
				post = entry['sql']['union']
			elif entry['sql']['except'] is not None:
				print("detected except clause!")
				has_except = True
				post = entry['sql']['except']

			if has_intersect or has_union or has_except:
				print("conds: ")
				print(post['from']['conds'])
				print("table_units: ")
				post_prev_tablist = [meta['table_names_original'][item[1]] for item in post['from']['table_units']]
				print(post['from']['table_units'])
				print(post_prev_tablist)
				print("")
				change_post_str = input('change from clauses in post-subquery?')
				if 'yes' in change_post_str:
					post_sql_from = get_from_info(table_dct, meta)
					post['from'] = post_sql_from
					if has_intersect:
						entry['sql']['intersect'] = post
					elif has_union:
						entry['sql']['union'] = post
					elif has_except:
						entry['sql']['except'] = post
					else:
						raise AssertionError
				else:
					pass
			else:
				pass

			check_str = input('is that correct?    ')
			if 'yes' in check_str:
				check = True
				res.append(entry)

	assert len(res) == len(train_data)

	with open('./spider/spider/train_spider_corrected.json', 'w') as fp:
		json.dump(res, fp, indent=4)

	with open('./spider/spider/sql_from_corrected_entry_idxs.json', 'w') as fp:
		json.dump(chosen.tolist(), fp)

	print("finished!")


if __name__ == '__main__':
	args = parser.parse_args()
	main(args)