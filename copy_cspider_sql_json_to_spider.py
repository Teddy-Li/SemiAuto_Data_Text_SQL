import json
import copy


def copy_back(correct_sql, spider_sql):
	# copy back the values in 'having' clause of the SPIDER entry
	for hv_idx in range(len(spider_sql['having'])):
		if isinstance(spider_sql['having'][hv_idx], str):
			assert isinstance(correct_sql['having'][hv_idx], str)
			continue
		if isinstance(spider_sql['having'][hv_idx][3], dict):
			assert isinstance(correct_sql['having'][hv_idx][3], dict)
			correct_sql['having'][hv_idx][3] = copy_back(copy.deepcopy(correct_sql['having'][hv_idx][3]),
														 copy.deepcopy(spider_sql['having'][hv_idx][3]))
		elif isinstance(spider_sql['having'][hv_idx][3], list):
			pass
		else:
			correct_sql['having'][hv_idx][3] = copy.deepcopy(spider_sql['having'][hv_idx][3])

		if isinstance(spider_sql['having'][hv_idx][4], dict):
			assert isinstance(correct_sql['having'][hv_idx][4], dict)
			correct_sql['having'][hv_idx][4] = copy_back(copy.deepcopy(correct_sql['having'][hv_idx][4]),
														 copy.deepcopy(spider_sql['having'][hv_idx][4]))
		elif isinstance(spider_sql['having'][hv_idx][4], list):
			pass
		else:
			correct_sql['having'][hv_idx][4] = copy.deepcopy(spider_sql['having'][hv_idx][4])

	for wh_idx in range(len(spider_sql['where'])):
		if isinstance(spider_sql['where'][wh_idx], str):
			assert isinstance(correct_sql['where'][wh_idx], str)
			continue
		if isinstance(spider_sql['where'][wh_idx][3], dict):
			assert isinstance(correct_sql['where'][wh_idx][3], dict)
			correct_sql['where'][wh_idx][3] = copy_back(copy.deepcopy(correct_sql['where'][wh_idx][3]),
														copy.deepcopy(spider_sql['where'][wh_idx][3]))
		elif isinstance(spider_sql['where'][wh_idx][3], list):
			pass
		else:
			correct_sql['where'][wh_idx][3] = copy.deepcopy(spider_sql['where'][wh_idx][3])

		if isinstance(spider_sql['where'][wh_idx][4], dict):
			assert isinstance(correct_sql['where'][wh_idx][4], dict)
			correct_sql['where'][wh_idx][4] = copy_back(copy.deepcopy(correct_sql['where'][wh_idx][4]),
														copy.deepcopy(spider_sql['where'][wh_idx][4]))
		elif isinstance(spider_sql['where'][wh_idx][4], list):
			pass
		else:
			correct_sql['where'][wh_idx][4] = copy.deepcopy(spider_sql['where'][wh_idx][4])

	if spider_sql['intersect'] is not None:
		assert spider_sql['union'] is None and spider_sql['except'] is None
		assert correct_sql['intersect'] is not None and \
			   correct_sql['union'] is None and correct_sql['except'] is None
		correct_sql['intersect'] = copy_back(copy.deepcopy(correct_sql['intersect']),
											 copy.deepcopy(spider_sql['intersect']))
	if spider_sql['union'] is not None:
		assert spider_sql['intersect'] is None and spider_sql['except'] is None
		assert correct_sql['union'] is not None and \
			   correct_sql['intersect'] is None and correct_sql['except'] is None
		correct_sql['union'] = copy_back(copy.deepcopy(correct_sql['union']),
										 copy.deepcopy(spider_sql['union']))
	if spider_sql['except'] is not None:
		assert spider_sql['intersect'] is None and spider_sql['union'] is None
		assert correct_sql['except'] is not None and \
			   correct_sql['intersect'] is None and correct_sql['union'] is None
		correct_sql['except'] = copy_back(copy.deepcopy(correct_sql['except']),
										 copy.deepcopy(spider_sql['except']))
	return correct_sql


def main():
	with open('./spider/spider/train_spider.json', 'r') as fp:
		spider_jsons = json.load(fp)

	with open('./CSpider/train.json', 'r') as fp:
		cspider_jsons = json.load(fp)

	assert len(spider_jsons) == 7000

	new_spider_jsons = []

	for e_idx in range(len(spider_jsons)):
		spider_entry = spider_jsons[e_idx]
		cspider_entry = cspider_jsons[e_idx]
		if spider_entry['query'] != cspider_entry['query']:
			print('idx: ', e_idx)
		assert len(spider_entry['sql']['having']) == len(cspider_entry['sql']['having'])
		assert len(spider_entry['sql']['where']) == len(cspider_entry['sql']['where'])

		correct_sql = copy.deepcopy(cspider_entry['sql'])
		correct_sql = copy_back(copy.deepcopy(correct_sql), copy.deepcopy(spider_entry['sql']))
		spider_entry['sql'] = copy.deepcopy(correct_sql)
		new_spider_jsons.append(spider_entry)

	with open('./spider/spider/train_spider_withc.json', 'w') as fp:
		json.dump(new_spider_jsons, fp, indent=4)


if __name__ == '__main__':
	main()
