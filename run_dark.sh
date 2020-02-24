source activate env3
rm -r saved_results

python -u grammar_rules_jan.py -d 51
python -u grammar_rules_jan.py -d 42
python -u grammar_rules_jan.py -d 138

Echo "finished"