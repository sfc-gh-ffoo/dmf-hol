from snowflake.snowpark.context import get_active_session
import streamlit as st
import pandas as pd
import altair as alt

session = get_active_session()
st.set_page_config(layout="wide")
st.title("Data Metric Function Manager")


tab1, tab2 = st.tabs(["Apply DMFs", "View Results"])

with tab1:
    st.header("Apply Data Metric Functions")
    
    # Database/Schema selection
    db = st.selectbox("Database", [row['name'] for row in session.sql("SHOW DATABASES").collect()])
    gov_schema = st.selectbox("Governance Schema", [row['name'] for row in session.sql(f"SHOW SCHEMAS IN {db}").collect()])

    # List DMFs in selected schema
    dmfs = session.sql(f"SHOW DATA METRIC FUNCTIONS IN SCHEMA {db}.{gov_schema}").collect()
    dmf_names = [f"{db}.{gov_schema}.{row['name']}" for row in dmfs]
    
    if dmfs:
        selected_dmf = st.selectbox("Select DMF", dmf_names)
        
        # Table/Column selection
        target_schema = st.selectbox("Target Schema", [row['name'] for row in session.sql(f"SHOW SCHEMAS IN {db}").collect()])
        table = st.selectbox("Target Table", [row['name'] for row in session.sql(f"SHOW TABLES IN {db}.{target_schema}").collect()])

        no_tables = [row['name'] for row in session.sql(f"SHOW TABLES IN {db}.{target_schema}").collect()]

        preview_df = session.sql(
        f"SELECT * FROM {db}.{target_schema}.{table} LIMIT 10").to_pandas()
        st.subheader(f"Data Preview of {table}")
        st.dataframe(preview_df, use_container_width=True)
        columns = [row['name'] for row in session.sql(f"DESC TABLE {db}.{target_schema}.{table}").collect()]
        column = st.selectbox("Target Column", columns)
        
        if st.button("Apply DMF"):
            try:
                session.sql(f"""
                    ALTER TABLE {db}.{target_schema}.{table} 
                    ADD DATA METRIC FUNCTION {selected_dmf} 
                    ON ({column})
                """).collect()
                st.success(f"Applied {selected_dmf.split('.')[-1]} to {table}.{column}")
            except Exception as e:
                st.error(f"Error: {str(e)}")
    else:
        st.warning("No DMFs found in selected schema")

with tab2:
    st.header("Visualize Metric Results")
    
    # Database/Table selection
    db_results = st.selectbox("Results Database", [row['name'] for row in session.sql("SHOW DATABASES").collect()])
    table_results = st.selectbox("Table to Monitor", [row['name'] for row in session.sql(f"SHOW TABLES IN DATABASE {db_results}").collect()])
    
    # Get metric results
    results = session.sql(f"""
        SELECT measurement_time, metric_name, value 
        FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
        WHERE table_name = '{table_results}'
        ORDER BY measurement_time
    """).to_pandas()
    

    if not results.empty:
        # Create a select box to choose the metric function
        metric_options = results['METRIC_NAME'].unique()
        selected_metric = st.selectbox("Select Data Metric Function to View", metric_options)
        
        # Filter results for the selected metric
        filtered = results[results['METRIC_NAME'] == selected_metric]
        
        # Show as bar graph (e.g., VALUE by MEASUREMENT_TIME)
        bar = alt.Chart(filtered).mark_bar().encode(
            x='MEASUREMENT_TIME:T',
            y='VALUE:Q',
            tooltip=['MEASUREMENT_TIME', 'VALUE']
        ).properties(
            width='container',
            height=400
        )
        st.altair_chart(bar, use_container_width=True)
    else:
        st.info("No metrics recorded for selected table")
