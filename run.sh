#!/bin/bash

verbose=$1

source activate env3
rm -r saved_results
for ((i=0; i<166; ++i))
do
echo $i
if ((i != 58))
then
python -u generator.py -d $i -v "${verbose}"
fi
done
echo "finished"