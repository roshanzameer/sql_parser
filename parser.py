import sqlparse
import pandas as pd
from sql_metadata import Parser
from collections.abc import Iterable

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


def read_hql_file(file_path):
    with open(file_path, 'r') as f:
        raw_hql = f.readlines()
        return raw_hql

def filter_hive_keywords(line):
    if 'set ' in line or 'USE ' in line or 'SET ' in line:
        return ''
    else:
        return line.strip()


def check_alias(parse):
    alias = parse.columns_aliases
    if not alias:
        return False
    else:
        return True
    
def get_table_name(parse):
    try:
        table = parse.tables
        return table[0]
    except:
        return ' . '


def flatten(column_list):
    for col in column_list:
        if isinstance(col, Iterable) and not isinstance(col, (str, bytes)):
            yield from flatten(col)
        else:
            yield col
    
def inner_where(sql_multi):
    sql_multi=sql_multi.replace('tt2','')
    sql_multi=sql_multi.replace('tt3','')
    sql_multi=sql_multi.replace('as','')
    sql_where=sql_multi
    #print(sql_where)
    sql_where = sqlparse.format(sql_where, reindent=True).split('\n')
    Lines=sql_where
    #print(Lines)
    count=0
    flag=0
    is_where=False
    main_where=""
    for line in Lines:
        line=line.lstrip()
        print(line)
        count+=1
        if 'where' in line:
            main_where+="{}".format(line.strip(''))
            flag=1
        if line.startswith('and') and flag==1:
            if 'where' in main_where:
                if ")" in line:
                    main_where+="{}".format(line.strip(''))
                    flag=0
                    continue
        if line.startswith('and') and ")," in line:
            main_where+="{}".format(line.strip(''))
        
            
    return main_where


def remove_create(sql):
    try:
        index_slice = []
        create_alias = False
        if 'SELECT' in sql or 'select' in sql:
            if 'STORED' in sql:
                split_sql = sql.split('\n')
                for i, line in enumerate(split_sql):
                    if 'CREATE' in line:
                        index_slice.append(i)
                    if ')' in line:
                        if not create_alias:
                            create_alias = True
                            index_slice.append(i)
                            break
            
                if create_alias:
                    #print (split_sql[0:i+2])
                    for line in enumerate(split_sql):
                        del split_sql[0:i+2]
                        print('DONE: REMOVE CREATE 1')
                        #print (split_sql)
                        return '\n'.join(split_sql)
            else:
                split_sql = sql.split('\n')
                for i, line in enumerate(split_sql):
                    if 'CREATE' in line:
                        index_slice.append(i)
                    if 'as' in line or 'AS' in line:
                        if not create_alias:
                            create_alias = True
                            index_slice.append(i)
                            break
                if create_alias:
                    del split_sql[0:i+1]
                    print('DONE: REMOVE CREATE 2')
                    return '\n'.join(split_sql)
    except Exception as e:
        #print('CANNOT SPLIT: ',e, sql)
        return sql
      
        
def get_where(sql):
    is_where = False
    is_group = False
    where_clause = ''
    sql_where = sqlparse.format(sql, reindent=True).split('\n')
    for line in sql_where:
        if '--' not in line:
            if 'WHERE' in line.upper():
                where_clause = where_clause + ' '.join(line.split(' ')[1:])
                is_where=True
            elif is_where and 'GROUP BY' not in line.upper():
                where_clause = where_clause + line
            if 'GROUP BY' in line.upper():
                break
    return where_clause


def get_direct_feature_table(sql):
    try:
        parse = Parser(sql) 
        #print('GET: ', sql)
        columns = parse.columns_dict['select']
        columns = list(flatten(columns))
        wheres = get_where(sql)
        #all_columns = columns
        all_columns = [i for n, i in enumerate(columns) if i not in columns[:n]]
        #all_columns = list(set(list(flatten(columns))))
        #print(all_columns)
        table = []
        for entity in all_columns:
            fin_entity = []
            if '.' in entity:
                entities = entity.split('.')
                try:
                    if len(entities) == 3:
                        fin_entity = [entities[0], entities[1], entities[2]]
                    elif len(entities) == 2:
                        fin_entity = ['', entities[0], entities[1]]
                except:
                    fin_entity = [entities[0], entities[1]]
                fin_entity.append(wheres)
                table.append(fin_entity)
            else:
                table_name = get_table_name(parse)
                fin_entity = table_name.split('.') + [entity]
                fin_entity.append(wheres)
                table.append(fin_entity)
        #print(table)
        return table
    except Exception as e:
        #print("Could not the process the below HQL: ", e)
        #print('HQL :', sql)
        #if 'SELECT' in sql or 'select' in sql:
        #    print("## ", sql, "\n")
        #traceback.print_exc()
        return []  
        
def process_complex_hql(raw_hql):

    #raw_hql = read_hql_file(file_path)
    #print(raw_hql)
    filtered_query = filter(filter_hive_keywords, raw_hql)
    raw_query = list(filtered_query)
    raw_query = ' '.join(raw_query)
    raw_query = raw_query.replace('"', "'")
    raw_query = raw_query.replace('${db_inp_data}', 'db_inp_data')
    raw_query = raw_query.replace('${db_dma_data}', 'db_dma_data')
    raw_query = raw_query.replace('${tbl_dma_data}', 'tbl_dma_data')   
    raw_query = raw_query.replace('${db_final_score}', 'db_final_score')
    raw_query = raw_query.replace('${tbl_final_score}', 'tbl_final_score')
    raw_query = raw_query.replace('${db_line_data}', 'db_line_data')
    raw_query = raw_query.replace('${tbl_line_data}', 'tbl_line_data')
    raw_query = raw_query.replace('${db_ds_data}', 'db_ds_data')

    multi_hql = raw_query.split(';')
    #return multi_hql
    #print(len(multi_hql))
    table = []
    for query in multi_hql:
        query = query.replace('"', "'")
        #query_pre = remove_create(query)
        #print (query_pre)
        table.extend(get_direct_feature_table(query))
    #return table
    #table_post_process = list(map(lambda x: x if len(x) == 4 else None, table))
    tab_final = [entity for entity in table if entity]
    df = pd.DataFrame(tab_final, columns=['schema_name', 'table_name', 'field_name', 'where_clause'])
    #df['WHERE'] = df['WHERE'].str.replace(r'where|WHERE', '')
    #dfStyler = df.style.set_properties(**{'text-align': 'left'})
    #df=dfStyler.set_table_styles([dict(selector='th', props=[('text-align', 'left')])])
    #df=df.style()
    print('DF returned')
    return df


def read_csv_file(file_path):
    df1=pd.read_csv(file_path)
    return df1


def calc_efficiency(fields, df, csv_df):
    print(fields)
    Matched_Features = pd.merge(df, 
                      csv_df, 
                      on=fields, 
                      how='inner')
    efficiency=len(Matched_Features.index)/len(csv_df.index)*100
    return round(efficiency,2)


