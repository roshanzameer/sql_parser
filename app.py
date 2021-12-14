import pandas as pd
import time
import streamlit as st
from parser import *
import json
from datetime import datetime
from io import StringIO 

sql_query = None

add_selectbox = st.sidebar.selectbox(
    "Please select one of the below",
    ("Extract Features", "Calculate Efficiency")
)
if add_selectbox == 'Extract Features':
    st.title('Feature Extractor')
    hql_file = st.file_uploader('Please upload the HQL File', type=None)
    if hql_file:
        stringio = StringIO(hql_file.getvalue().decode('utf-8'))
        sql_query = stringio.read()

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
                time.sleep(2)
            st.success('Your Query has been processed!')
            hql_file = [line.decode('utf-8') for line in hql_file.readlines()]
            df = process_complex_hql(hql_file)
            df

            export_data = df.to_csv(index=False)
                        

            st.download_button(label='Export the data', 
                                data=export_data,
                                file_name=f'feature_data_{datetime.now()}.csv',
                                )

else:
    st.title('Efficiency Calculator')
    st.markdown('**Please ensure the columns "schema_name", "table_name", and "field_name" are present in the feature CSV file.**')
    files = st.file_uploader('Please upload the HQL and the Feature CSV File', type=None, accept_multiple_files=True)
    hql_file = None
    csv_file = None
    if files:
        for file in files:
            if 'hql' in file.name:
                hql_file = [line.decode('utf-8') for line in file.readlines()]
            else:
                csv_file = [line.decode('utf-8') for line in file.readlines()]
                csv_file = [line.split(',') for line in csv_file]
        if csv_file:
            csv_df = pd.DataFrame(csv_file)
            csv_df = csv_df.rename(columns=csv_df.iloc[0])
            csv_df = csv_df.drop(csv_df.index[0])
            #csv_df
        if hql_file:
            feature_df = process_complex_hql(hql_file)
        
            with st.expander("CSV Feature DF"):
                csv_df
            with st.expander("Feature Extractor DF"):
                feature_df

            fields = st.multiselect(label='Select the fields on which to calculate the Extraction efficiency', 
                                    options=['schema_name','table_name','field_name'],
                                    default='field_name')
            calculate_efficiency = st.button('Calculate Efficiency')
            if calculate_efficiency:
                with st.spinner('Calculation is in process.....'):
                    time.sleep(3)
                efficiency = calc_efficiency(fields, feature_df, csv_df)
                if efficiency:
                    st.metric(label='The extraction efficiency is', value=f'{efficiency} %', delta=None)
