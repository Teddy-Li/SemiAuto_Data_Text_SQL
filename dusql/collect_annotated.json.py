import json
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--path', type=str, default='./annotation_result')
parser.add_argument('-c', '--canonical', type=str, default='../dusql_converted/canonicals_all_val.json')
args = parser.parse_args()

file_names = os.listdir(args.path)

with open(args.canonical, 'r') as fp:
	canonical_file = json.load(fp)

res_files = []

stuid_dict = {}
entry_idxs = []

for fn in file_names:
	with open(os.path.join(args.path, fn), 'r', encoding='utf-8') as fp:
		if '.json' not in fn:
			continue
		jsondict = json.load(fp)
		print(jsondict['entry_idx'])
		if jsondict['entry_idx'] in entry_idxs:
			continue
		entry = None
		for e in canonical_file:
			if int(e['global_idx']) == int(jsondict['entry_idx']):
				entry = e
		assert entry is not None
		entry['question'] = jsondict['question']
		print(entry['question'])
		entry['sensible'] = True if jsondict['sensible'] == 'yes' else False
		entry['concise'] = True if jsondict['concise'] == 'yes' else False
		entry['complex'] = True if jsondict['complex'] == 'yes' else False
		entry['trouble_understand'] = int(jsondict['trouble_understand'])
		entry['worldly'] = True if jsondict['worldly'] == 'yes' else False
		entry['worker_id'] = jsondict['stu_id']
		entry_idxs.append(jsondict['entry_idx'])
		if jsondict['stu_id'] not in stuid_dict:
			stuid_dict[jsondict['stu_id']] = 1
		else:
			stuid_dict[jsondict['stu_id']] += 1
		res_files.append(entry)

with open('./annotated_testm.json', 'w', encoding='utf-8') as fp:
	json.dump(res_files, fp, indent=4, ensure_ascii=False)

with open('./testm_stuid_dict.json', 'w') as fp:
	json.dump(stuid_dict, fp, indent=4)
