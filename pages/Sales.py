import streamlit as st
import pandas as pd
import plotly.express as px
import os
import time
import logging
from controlflow import Agent, Task as CFTask
from pydantic import BaseModel
# from IPython.display import Markdown, display
from prefect import task, flow
from typing import ClassVar
from prefect.artifacts import (
    create_progress_artifact,
    update_progress_artifact,
    create_link_artifact,
    create_markdown_artifact
)
logging.basicConfig(level=logging.DEBUG)
#task_progress = 0.0
# Debug information
print("Current working directory:", os.getcwd())
print("File exists:", os.path.exists('../data/OnlineSalesData.csv'))

audit_agent = Agent(
    name="Auditor",
    model="gpt-3.5-turbo",
    description="An AI agent that is an expert in auditing, data analysis, and writing reports.",
    instructions="""
        Your primary goal is to validate the data, generate an audit report, and ensure that the data
        is ready for analysis. You will be working with the data provided to you,
        and you will need to validate it, generate a report one paragraph long.
    """,
)

class AuditReport(BaseModel):
    title: str
    markdown: str




@task(name="Load Sales Data", retries=3, retry_delay_seconds=10)
def load_data(uploaded_file):
    """
    Load the sales data from a CSV file.
    Returns:
        pandas.DataFrame: The loaded sales data.
    """
    if uploaded_file is not None:
        with st.spinner('Loading data...'):
            # Simulate a delay to show the spinner (remove this in production)
            time.sleep(2)
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file)
            st.write(df)
            if check_data(df):
                st.success('Data loaded successfully!')
                return df
            else:
                return st.error("Data is not ready for analysis.")

    else:
        st.error('No file uploaded. Please upload a CSV file.')
    
    return pd.DataFrame()  # Return an empty DataFrame if no file is uploaded or an error occurs
@task(name="Check Data", retries=2, retry_delay_seconds=5)
def check_data(df):
    """
    Check the data for any issues.
    Args:
        df (pandas.DataFrame): The sales data.
    """
    if df.empty:
        st.error("No data found in the uploaded file.")
        return False
    if "Total Revenue" not in df.columns:
        st.error("The 'Total Revenue' column is missing from the data.")
        return False
    if "Units Sold" not in df.columns:
        st.error("The 'Units Sold' column is missing from the data.")
        return False
    if "Product Category" not in df.columns:
        st.error("The 'Product Category' column is missing from the data.")
        return False
    if "Region" not in df.columns:
        st.error("The 'Region' column is missing from the data.")
        return False
    else:
        st.success("Data is ready for analysis.")
        st.write(df.head())
        return True

@task(name="Display Overview")
def display_overview(df):
    """
    Display an overview of the sales data.
    Args:
        df (pandas.DataFrame): The sales data.
    """
    st.header('Overview')
    with st.spinner('Loading data...'):
        time.sleep(2)
        if check_data(df):
            st.write(f"Total Transactions: {len(df)}")
            st.write(f"Total Revenue: ${df['Total Revenue'].sum():,.2f}")
        else:
            plot_category_sales(df)
    st.write(f"Average Order Value: ${df['Total Revenue'].mean():,.2f}")

@task(name="Plot Category Sales")
def plot_category_sales(df):
    """
    Plot sales by product category.
    Args:
        df (pandas.DataFrame): The sales data.
    """
    st.header('Sales by Product Category')
    category_sales = df.groupby('Product Category')['Total Revenue'].sum().sort_values(ascending=False)
    fig_category = px.bar(category_sales, x=category_sales.index, y='Total Revenue', 
                          title='Total Revenue by Product Category')
    st.plotly_chart(fig_category)

@task(name="Plot Sales Trend")
def plot_sales_trend(df):
    """
    Plot the sales trend over time.
    Args:
        df (pandas.DataFrame): The sales data.
    """
    st.header('Sales Trend Over Time')
    df['Date'] = pd.to_datetime(df['Date'])
    daily_sales = df.groupby('Date')['Total Revenue'].sum().reset_index()
    fig_trend = px.line(daily_sales, x='Date', y='Total Revenue', title='Daily Sales Trend')
    st.plotly_chart(fig_trend)

@task(name="Plot Top Products")
def plot_top_products(df):
    """
    Plot the top selling products.
    Args:
        df (pandas.DataFrame): The sales data.
    """
    st.header('Top Selling Products')
    top_products = df.groupby('Product Name')['Units Sold'].sum().sort_values(ascending=False).head(10)
    fig_products = px.bar(top_products, x=top_products.index, y='Units Sold', 
                          title='Top 10 Selling Products')
    st.plotly_chart(fig_products)

@task(name="Plot Regional Sales")
def plot_regional_sales(df):
    """
    Plot sales by region.
    Args:
        df (pandas.DataFrame): The sales data.
    """
    st.header('Sales by Region')
    region_sales = df.groupby('Region')['Total Revenue'].sum().sort_values(ascending=False)
    fig_region = px.pie(region_sales, values='Total Revenue', names=region_sales.index, 
                        title='Sales Distribution by Region')
    st.plotly_chart(fig_region)

@task(name="Plot Payment Methods")
def plot_payment_methods(df):
    """
    Plot the distribution of payment methods.
    Args:
        df (pandas.DataFrame): The sales data.
    """
    st.header('Payment Method Analysis')
    payment_method = df['Payment Method'].value_counts()
    fig_payment = px.pie(payment_method, values=payment_method.values, names=payment_method.index, 
                         title='Payment Method Distribution')
    st.plotly_chart(fig_payment)

@task(name="Display Interactive Explorer")
def display_interactive_explorer(df):
    """
    Display an interactive data explorer.
    Args:
        df (pandas.DataFrame): The sales data.
    """
    st.header('Interactive Data Explorer')
    selected_columns = st.multiselect('Select columns to display', df.columns.tolist(), 
                                      default=['Transaction ID', 'Date', 'Product Name', 'Total Revenue'])
    st.dataframe(df[selected_columns])
    

@task(name="Report Control Flow Analysis")
def report_control_flow_analysis(markdown, title):
    st.header(title)
    st.markdown(markdown)
    create_markdown_artifact(
        key="audit-report",
        markdown=markdown,
        description=title,
    )


@flow(name="Analyze Data")
def analyze_data(df):
    """
    Analyze the sales data.
    Args:
        df (pandas.DataFrame): The sales data.
    """
    progress_artifact_id = create_progress_artifact(
        progress=0.0,
        description="Indicates the progress of fetching data in batches.",
    )
    display_overview(df)
    task_progress = 0.0
    print("task_progress: ", task_progress)
    update_progress_artifact(
        progress=task_progress + 15,
        artifact_id=progress_artifact_id,
        description="Overview analysis completed successfully.",
    )
    time.sleep(5)
    plot_category_sales(df)
    update_progress_artifact(
        progress=task_progress + 15,
        artifact_id=progress_artifact_id,
        description="Category sales analysis completed successfully.",
    )
    time.sleep(5)
    plot_sales_trend(df)
    update_progress_artifact(
        progress=task_progress + 15,
        artifact_id=progress_artifact_id,
        description="Sales trend analysis completed successfully.",
    )
    time.sleep(5)
    plot_top_products(df)
    update_progress_artifact(
        progress=task_progress + 15,
        artifact_id=progress_artifact_id,
        description="Top products analysis completed successfully.",
    )
    time.sleep(5)
    plot_regional_sales(df)
    update_progress_artifact(
        progress=task_progress + 15,
        artifact_id=progress_artifact_id,
        description="Regional sales analysis completed successfully.",
    )
    time.sleep(5)
    plot_payment_methods(df)
    update_progress_artifact(
        progress=task_progress + 15,
        artifact_id=progress_artifact_id,
        description="Payment method analysis completed successfully.",
    )
    time.sleep(5)
    display_interactive_explorer(df)
    
    data_str = df.to_csv(index=False)

    report = CFTask(
        """
You are an expert data analyst. Analyze the provided sales data and generate a markdown report summarizing key insights. The report should include:

- Total Revenue
- Total Units Sold
- Average Revenue per Unit
- Top-selling product categories
- Regions with the highest sales
- Most common payment methods

The data is provided in CSV format in the context variable `data`.
        """,
        context=dict(
            data=data_str,
        ),
        result_type=AuditReport,
        agents=[audit_agent],
    )
    try:
        report.run()
    except Exception as e:
        st.error(f"An error occurred while generating the report: {e}")
        return
    
    report_control_flow_analysis(report.result.markdown, report.result.title)
    # Create a markdown artifact for progress overview

    # Update the progress artifact to 100%
    update_progress_artifact(
        progress=100,
        artifact_id=progress_artifact_id,
        description="Data analysis completed successfully.",
    )

@flow(name="Sales Analysis Flow")
def main():
    """
    Main function to run the Streamlit app.
    """
    st.title('Online Sales Data Analysis')
    st.write("This app allows you to analyze online sales data. You can upload a CSV file containing the sales data or use the default data.")
    with st.form(key='upload_form', clear_on_submit=True):
        uploaded_file = st.file_uploader("Choose a CSV file", type=['csv','xlsx','xls'], accept_multiple_files=False)
        submit_button = st.form_submit_button(label='Submit')
        
    if submit_button and uploaded_file is not None:
        uploaded_file.seek(0)
        df = load_data(uploaded_file)
        if not df.empty:
            print("Data loaded successfully!")
            analyze_data(df)
        else:
            st.error("No data found in the uploaded file.")
    else:
        st.error("Please upload a CSV file.")        


if __name__ == "__main__":
    main()
