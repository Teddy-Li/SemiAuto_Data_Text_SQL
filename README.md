# data_gen
## Code structure
* annotate.py: A command line annotation tool to annotate generated canonical utterances, for convenience in manual annotation.
* __examine_dataset.py__: A python script to look into certain details and statistics in SQL queries of both original SPIDER datasets and our generated datasets.
* fetch_db_split.py: A python script to find which databases are in dev set and which are in train set in SPIDER.
* __generator.py__: The core script to generate SQL queries paired with canonical natural language utterances in rule-based random manner.
* inspect_tables.py: A python script to check out table stats, retype all columns according to their actual type and assign columns with 'id' in their names as of 'id' type.
* json2csv.py: A python script to convert SPIDER-styled generated query pairs into a csv format ready for crowd sourcing.
* run.sh: Run this script to generate queries on all databases.
* run_dark.sh: Run this script to generate queries on the three put-aside databases, namely: concert_singer, pets_1 and car_1.
* survey.html: The survey webpage to use.
