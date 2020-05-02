import json
import os
import argparse
import random

parser = argparse.ArgumentParser()
parser.add_argument('-d', '--dark', type=bool, default=False)
parser.add_argument('-p', '--pilot', type=bool, default=False, help="whether this is for a pilot experiment; if so, "
																	"50 entries will be sampled out of all entries")
parser.add_argument('-r', '--run', type=bool, default=False)
parser.add_argument('-i', '--input', type=str, default='')
parser.add_argument('-o', '--output', type=str, default='')

args = parser.parse_args()
if args.dark:
	PATHS = ['concert_singer', 'pets_1', 'car_1']
	PATHS = [os.path.join('saved_results', path, 'qrys_saved.json') for path in PATHS]
	OUT_PATHS = [os.path.join('saved_results', path, 'input.csv') for path in PATHS]
elif args.run:
	PATHS = [os.path.join('saved_results', 'qrys_saved.json')]
	OUT_PATHS = [os.path.join('saved_results', 'input.csv')]
elif len(args.input) > 0:
	assert len(args.output) > 0
	PATHS = [args.input]
	OUT_PATHS = [args.output]
else:
	raise AssertionError

for path, out_path in zip(PATHS, OUT_PATHS):
	with open(path, 'r', encoding='utf-8') as fp:
		file = json.load(fp)

	if args.pilot:
		file = random.sample(file, k=50)

	with open(out_path, 'w', encoding='utf-8') as fp:
		fp.write('topic,sequence,gold,ref_sequence,ref_gold,answer,ref_answer,qryidx\n')
		for entry in file:
			dbname = entry['db_id'].split('_')
			n_dbname = []
			for w in dbname:
				if w not in ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0']:
					n_dbname.append(w)
			topic = ' '.join(n_dbname)
			topic = '\"'+topic+'\"'

			ref_sequence = []
			for item in entry['ref_question_sequence']:
				q = []
				item = item.split()
				for w in item:
					tocsv_w = None
					if w[0] == '@':
						tocsv_w = '<font color=red>'+w[1:]
						if tocsv_w.count('@') > 1 or tocsv_w.count('$') > 0:
							print(item)
							print(tocsv_w)
							raise AssertionError
						tocsv_w = tocsv_w.replace('@', '</font>')
					elif w[0] == '$':
						tocsv_w = '<font color=blue>'+w[1:]
						if tocsv_w.count('@') > 0 or tocsv_w.count('$') > 1:
							print(item)
							print(tocsv_w)
							raise AssertionError
						tocsv_w = tocsv_w.replace('$', '</font>')
					else:
						tocsv_w = w
						if tocsv_w.count('@') > 1 or tocsv_w.count('$') > 1:
							print(item)
							print(tocsv_w)
							raise AssertionError
						tocsv_w = tocsv_w.replace('@', '</font>')
						tocsv_w = tocsv_w.replace('$', '</font>')
					q.append(tocsv_w)
				q = ' '.join(q)
				ref_sequence.append(q)
			ref_sequence = ' <br> '.join(ref_sequence)
			ref_sequence = ref_sequence.replace('\"', '')
			ref_sequence = '\"'+ref_sequence+'\"'

			ref_gold = entry['ref_gold']
			ref_gold = '\"' + ref_gold + '\"'

			entry['ref_response'][0] = entry['ref_response'][0].replace('*', 'Everything')
			ref_response = '<table border=2> <tr> '
			for item in entry['ref_response'][0].split(','):
				ref_response += ' <th> '+item.strip().strip('"')+' </th> '
			ref_response += '</tr> <tr> '
			for line in entry['ref_response'][1:]:
				for item in line.split(','):
					ref_response += ' <th> ' + item.strip().strip('"')+' </th> '
			ref_response += ' </tr> </table>'
			ref_response = '\"' + ref_response + '\"'

			sequence = []
			for item in entry['question_sequence']:
				q = []
				item = item.split()
				for w in item:
					tocsv_w = None
					if w[0] == '@':
						tocsv_w = '<font color=red>'+w[1:]
						if tocsv_w.count('@') > 1 or tocsv_w.count('$') > 0:
							print(item)
							print(tocsv_w)
							raise AssertionError
						tocsv_w = tocsv_w.replace('@', '</font>')
					elif w[0] == '$':
						tocsv_w = '<font color=blue>'+w[1:]
						if tocsv_w.count('@') > 0 or tocsv_w.count('$') > 1:
							print(item)
							print(tocsv_w)
							raise AssertionError
						tocsv_w = tocsv_w.replace('$', '</font>')
					else:
						tocsv_w = w
						if tocsv_w.count('@') > 1 or tocsv_w.count('$') > 1:
							print(item)
							print(tocsv_w)
							raise AssertionError
						tocsv_w = tocsv_w.replace('@', '</font>')
						tocsv_w = tocsv_w.replace('$', '</font>')
					q.append(tocsv_w)
				q = ' '.join(q)
				sequence.append(q)
			sequence = ' <br> '.join(sequence)
			sequence = sequence.replace('\"', '')
			sequence = '\"'+sequence+'\"'

			gold = entry['question_gold']
			gold = '\"' + gold + '\"'

			entry['answer_sample'][0] = entry['answer_sample'][0].replace('*', 'Everything')
			answer_sample = '<table border=2> <tr> '
			for item in entry['answer_sample'][0].split(','):
				answer_sample += ' <th> ' + item.strip().strip('"') + ' </th> '
			answer_sample += '</tr> <tr> '
			for line in entry['answer_sample'][1:]:
				for item in line.split(','):
					answer_sample += ' <th> ' + item.strip().strip('"') + ' </th> '
			answer_sample += ' </tr> </table>'
			answer_sample = '\"' + answer_sample + '\"'

			qry_idx = entry['global_idx']
			qry_idx = '\"'+str(qry_idx)+'\"'

			res = [topic, sequence, gold, ref_sequence, ref_gold, answer_sample, ref_response, qry_idx]
			fp.write(','.join(res)+'\n')

