rm -r saved_results

python -u generator.py -m run -d 51
python -u generator.py -m run -d 42
python -u generator.py -m run -d 138

Echo "finished"