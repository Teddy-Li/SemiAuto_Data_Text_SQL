import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--path', type=str, default='spider_annotated_testm.csv')
parser.add_argument('-r', '--raw_json', type=str)
parser.add_argument('-c', '--raw_csv', type=str)
parser.add_argument('-o', '--out_path', type=str)