source activate env3
rm -r saved_results

python -u generator.py -d 51
python -u generator.py -d 42
python -u generator.py -d 138

Echo "finished"