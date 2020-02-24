source activate env3
rm -r saved_results
for ((i=0; i<166; ++i))
do
echo $i
python -u grammar_rules_jan.py -d $i
done
Echo "finished"