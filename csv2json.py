import json
import csv
import argparse
import nltk

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--path', type=str, default='spider_annotated_testm.csv')
parser.add_argument('-r', '--raw_json', type=str)
parser.add_argument('-c', '--raw_csv', type=str)
parser.add_argument('-o', '--out_path', type=str)

args = parser.parse_args()

with open(args.raw_json, 'r') as fp:
	json_files = json.load(fp)

topics = []
annotated = []
with open(args.path, 'r', newline='') as csvfile:
	reader = csv.DictReader(csvfile)
	for row in reader:
		if row['AssignmentStatus'] != 'Approved':
			continue
		if row['Input.topic'] not in topics:
			topics.append(row['Input.topic'])
		annotated.append(row)
print(topics)
raw = []
with open(args.raw_csv, 'r') as fp:
	reader = csv.DictReader(fp)
	for row in reader:
		raw.append(row)

res_files = []

complex_cnt = 0
complex_tcnt = 0
concise_cnt = 0
concise_tcnt = 0
sensible_cnt = 0
sensible_tcnt = 0
worldly_cnt = 0
worldly_tcnt = 0
trouble_cnt = 0
trouble_tcnt = 0

for row in annotated:
	global_idxs = []
	for ref_row in raw:
		if row['Input.sequence'] == ref_row['sequence']:
			global_idxs.append(ref_row['qryidx'])
	if len(global_idxs) != 1:
		raise AssertionError
	jsons = []
	for j in json_files:
		if j['global_idx'] == int(global_idxs[0]):
			jsons.append(j)
	if len(jsons) != 1:
		raise AssertionError
	res = jsons[0]
	res['question'] = row['Answer.question']
	res['question_toks'] = nltk.word_tokenize(row['Answer.question'])
	complex_tcnt += 1
	concise_tcnt += 1
	sensible_tcnt += 1
	worldly_tcnt += 1
	trouble_tcnt += 1
	if row['Answer.complex_f.no'].lower() == 'true':
		res['complex'] = False
	elif row['Answer.complex_t.yes'] == 'true':
		res['complex'] = True
		complex_cnt += 1
	else:
		res['complex'] = None
		complex_tcnt -= 1

	if row['Answer.concise_f.no'] == 'true':
		res['concise'] = False
	elif row['Answer.concise_t.yes'] == 'true':
		res['concise'] = True
		concise_cnt += 1
	else:
		res['concise'] = None
		concise_tcnt -= 1

	if row['Answer.sensible_f.no'] == 'true':
		res['sensible'] = False
	elif row['Answer.sensible_t.yes'] == 'true':
		res['sensible'] = True
		sensible_cnt += 1
	else:
		res['concise'] = None
		sensible_tcnt -= 1

	if row['Answer.worldly_f.no'] == 'true':
		res['worldly'] = False
	elif row['Answer.worldly_t.yes'] == 'true':
		res['worldly'] = True
		worldly_cnt += 1
	else:
		res['concise'] = None
		worldly_tcnt -= 1

	if row['Answer.trouble_understand_0.0'] == 'true':
		res['trouble_understand'] = 0
	elif row['Answer.trouble_understand_1.1'] == 'true':
		res['trouble_understand'] = 1
		trouble_cnt += 1
	elif row['Answer.trouble_understand_2.2'] == 'true':
		res['trouble_understand'] = 2
		trouble_cnt += 2
	elif row['Answer.trouble_understand_3.3'] == 'true':
		res['trouble_understand'] = 3
		trouble_cnt += 3
	elif row['Answer.trouble_understand_4plus.4more'] == 'true':
		res['trouble_understand'] = 4
		trouble_cnt += 4
	else:
		res['trouble_understand'] = None
		trouble_tcnt -= 1

	res_files.append(res)

with open(args.out_path, 'w') as fp:
	json.dump(res_files, fp, indent=4)


print("Complex rate: ", '%.4f' % (float(complex_cnt)/float(complex_tcnt)))
print("Concise rate: ", '%.4f' % (float(concise_cnt)/float(concise_tcnt)))
print("Sensible rate: ", '%.4f' % (float(sensible_cnt)/float(sensible_tcnt)))
print("Worldly rate: ", '%.4f' % (float(worldly_cnt)/float(worldly_tcnt)))
print("Trouble average: ", '%.4f' % (float(trouble_cnt)/float(trouble_tcnt)))
