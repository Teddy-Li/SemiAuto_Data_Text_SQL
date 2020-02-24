import json
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-d', '--dark', type=bool, default=False)

args = parser.parse_args()
if args.dark:
	PATHS = ['concert_singer', 'pets_1', 'car_1']
else:
	PATHS = ['']

for path in PATHS:
	with open(os.path.join('saved_results', path, 'qrys_saved.json'), 'r') as fp:
		file = json.load(fp)

	with open(os.path.join('saved_results', path, 'input.csv'), 'w') as fp:
		fp.write('topic,sequence,answer\n')
		for entry in file:
			dbname = entry['db_id'].split('_')
			n_dbname = []
			for w in dbname:
				if w not in ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0']:
					n_dbname.append(w)
			topic = ' '.join(n_dbname)
			topic = '\"'+topic+'\"'

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

			answer = ' <br> '.join(entry['answer_sample'])
			answer = answer.replace('\"', '')
			answer = '\"'+answer+'\"'
			res = [topic, sequence, answer]
			fp.write(','.join(res)+'\n')

