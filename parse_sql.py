import sys
from os.path import join, dirname, abspath

# dir_path = join(dirname(abspath(__file__)), 'sql_metadata', 'sql_metadata')
# print(dir_path)
# sys.path.insert(0, '/home/roshan.zameer/sql_project/sql_metadata/sql_metadata')
#from parser import Parser

from sql_metadata import Parser
import sqlparse


def format_query(sql):
    return sqlparse.format(sql, reindent=True, keyword_case='upper')


def get_direct_features(sql):
    return Parser(sql).columns

  
def get_transformed_features(sql):
    return Parser(sql).columns_aliases

  
def get_table_names(sql):
    return Parser(sql).tables

  
def get_table_aliases(sql):
    return Parser(sql).tables_aliases

  
def get_transformations(sql):
    trans_feats = Parser(sql).columns_aliases
    transformations = {}
    sql_tokens = sqlparse.format(sql, keyword_case='upper').split('\n')
    for feat in trans_feats:
        for col_token in sql_tokens:
            if feat in col_token:
                trans = col_token.split(f'AS {feat}')[0:-1]
                transformations[feat] = ' '.join(trans) 
    
    return transformations

