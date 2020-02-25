verbose = $1

source activate env3
rm -r saved_results
for ((i=0; i<166; ++i))
do
echo $i
python -u generator.py -d $i -v ${verbose}
done
Echo "finished"