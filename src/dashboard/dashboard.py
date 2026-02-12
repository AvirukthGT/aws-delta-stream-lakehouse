import streamlit as st
import awswrangler as wr
import pandas as pd
import plotly.express as px

# Config
st.set_page_config(page_title="Lakehouse Dashboard", layout="wide")
st.title("Live E-Commerce Lakehouse Dashboard")

# 1. Connect to Athena (Silver Layer)
@st.cache_data(ttl=60)  # Cache data for 1 minute
def load_data():
    query = """
    SELECT 
        city,
        category,
        order_date,
        count(*) as total_orders,
        sum(total_amount) as revenue
    FROM "ecommerce_silver_db"."fact_sales_enriched"
    GROUP BY city, category, order_date
    """
    # Run query in Athena
    df = wr.athena.read_sql_query(query, database="ecommerce_silver_db", ctas_approach=False)
    return df

try:
    with st.spinner('Fetching live data from S3 Lakehouse...'):
        df = load_data()
    
    # 2. Key Metrics Row
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Revenue", f"${df['revenue'].sum():,.2f}")
    col2.metric("Total Orders", f"{df['total_orders'].sum()}")
    col3.metric("Top City", df.groupby('city')['revenue'].sum().idxmax())

    # 3. Charts
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Revenue by City")
        fig_city = px.bar(df.groupby('city')['revenue'].sum().reset_index(), 
                          x='city', y='revenue', color='revenue')
        st.plotly_chart(fig_city, use_container_width=True)

    with col_right:
        st.subheader("Sales by Category")
        fig_cat = px.pie(df, values='revenue', names='category')
        st.plotly_chart(fig_cat, use_container_width=True)

    # 4. Raw Data View
    with st.expander("View Raw Data (Silver Layer)"):
        st.dataframe(df)

except Exception as e:
    st.error(f"Error connecting to Athena: {e}")