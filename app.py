import pandas as pd
import time
import streamlit as st
from parse_sql import *
import json
from datetime import datetime

st.title('Feature Extractor')

sql_query = None

st.write('Please enter the SQL query in the box below')
sql_query = st.text_area("", height=400)

if sql_query:
    formatted_query = format_query(sql_query)

    if st.checkbox('Show Formatted Query?'):
        st.code(formatted_query)

st.text('\n')
st.text('\n')

if sql_query:
    process = st.button('Process Query')

    if process:

        with st.spinner('Your query is being processed.....'):
            time.sleep(3)
        st.success('Your Query has been processed!')

        table_name = get_table_names(sql_query)
        table_aliases = get_table_aliases(sql_query)
        direct_features = get_direct_features(sql_query)
        transformed_feats = get_transformations(sql_query)
        tf = pd.DataFrame(transformed_feats.items(), columns=['Base Features', 'Transformations'])

        #options = st.selectbox('View the Entities:', ('Transformed Features', 'Table Name', 'Direct Features', 'Table Alias'))
        with st.expander("Table Name"):
            st.table(table_name)

        with st.expander("Table Alias"):
            st.table(table_aliases)

        with st.expander("Direct Features"):
            st.table(direct_features)
        
        with st.expander("Transformed Features"):
            st.table(tf)


        export_data = {'table': table_name,
                        'table_alias': table_aliases,
                        'direct_features': direct_features,
                        'transformed_features': transformed_feats
                    }

        st.download_button(label='Export the data', 
                            data=json.dumps(export_data),
                            file_name=f'feature_data_{datetime.now()}.json',
                            )
