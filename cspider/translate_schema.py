import json
import time
import hashlib
import copy
import requests

api_url = "http://api.fanyi.baidu.com/api/trans/vip/translate"
my_appid = "20200507000442561"
cyber = "LL7vumyns4bGkZgNEyAI"


def request_for_dst(word):
	salt = str(time.time())[:10]
	final_sign = str(my_appid) + word + salt + cyber
	final_sign = hashlib.md5(final_sign.encode("utf-8")).hexdigest()
	paramas = {
		'q': word,
		'from': 'en',
		'to': 'zh',
		'appid': '%s' % my_appid,
		'salt': '%s' % salt,
		'sign': '%s' % final_sign
	}
	my_url = api_url + '?appid=' + str(
		my_appid) + '&q=' + word + '&from=' + 'en' + '&to=' + 'spa' + '&salt=' + salt + '&sign=' + final_sign
	response = requests.get(api_url, params=paramas).content
	time.sleep(1)
	content = str(response, encoding="utf-8")
	json_reads = json.loads(content)
	try:
		return json_reads['trans_result'][0]['dst']
	except Exception as e:
		print(json_reads)
		print(e)
		raise


with open('tables_mod.json', 'r') as fp:
	schemas = json.load(fp)

schemas_original = copy.deepcopy(schemas)

length = 0
# get total length
for db in schemas:
	length += len(db['table_names'])
	length += len(db['column_names'])

print("total length: ", length)

translated_schemas = []

for didx, db in enumerate(schemas):
	print("Began on database %d: " % didx, db['db_id'])
	table_names = db['table_names']
	column_names = []
	for col in db['column_names']:
		if col[0] < 0:
			column_names.append('*')
		else:
			col_n = table_names[col[0]] + "'s "
			if len(col[1]) >= 2 and col[1][-2:] == 'id':
				col_n += col[1][:-2]
			else:
				col_n += col[1]
			column_names.append(col_n)
	print("Length of this DB: ", len(table_names)+len(column_names))
	translated_table_names = []
	translated_column_names = []

	for item in table_names:
		chinese_name = request_for_dst(item)
		translated_table_names.append(chinese_name)

	for item in column_names:
		chinese_name = request_for_dst(item)
		translated_column_names.append(chinese_name)
	db['table_names'] = translated_table_names
	assert len(db['column_names']) == len(translated_column_names)
	for i in range(len(db['column_names'])):
		if len(db['column_names'][i][1]) > 3 and db['column_names'][i][1][-3:] == ' id':
			db['column_names'][i][1] = translated_column_names[i]+' id'
		elif db['column_names'][i][1] == 'id':
			db['column_names'][i][1] = translated_table_names[db['column_names'][i][0]] + ' id'
		else:
			db['column_names'][i][1] = translated_column_names[i]
	translated_schemas.append(db)

with open('tables_mod_original.json', 'w') as fp:
	json.dump(schemas_original, fp, indent=4)

with open('tables_mod.json', 'w', encoding='utf-8') as fp:
	json.dump(translated_schemas, fp, indent=4, ensure_ascii=False)