import json
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--path', type=str, help='path')
parser.add_argument('-t', '--table_p', type=str, default='../spider/spider/tables_mod.json')
parser.add_argument('-f', '--start_from', type=int, default=0, help='start from')
args = parser.parse_args()

with open(os.path.join('saved_results', args.path, 'qrys_saved.json'), 'r') as fp:
	saved_qrys = json.load(fp)

with open(args.table_p, 'r') as fp:
	tables = json.load(fp)

try:
	with open(os.path.join('saved_results', args.path, 'qrys_saved_annotated.json'), 'r') as fp:
		annotated = json.load(fp)
except Exception as e:
	annotated = []


last_idx = args.start_from
for idx, item in enumerate(saved_qrys[args.start_from:]):
	db = None
	for t in tables:
		if item['db_id'] == t['db_id']:
			db = t
			break
	assert db is not None
	print('')
	print('Entry %d / %d' % (idx+1, len(saved_qrys)))
	print('database: ', item['db_id'])
	#print('sql: ')
	#print(item['query'])
	#print('full question: ')
	#print(item['question'])
	#print('')
	print('question in sequence: ')
	for s in item['question_sequence']:
		print(s)
	print('')
	print('answer sample: ')
	print(item['answer_sample'])

	item['annotated_c_english'] = input('Annotation: ')
	annotated.append(item)
	last_idx = idx
	quit = input('Quit?	')
	if len(quit) > 0:
		with open(os.path.join('saved_results', args.path, 'qrys_saved_annotated.json'), 'w') as fp:
			json.dump(annotated, fp)
		print('Stopped at index %d' % last_idx)
		break