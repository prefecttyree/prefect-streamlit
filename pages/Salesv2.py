import streamlit as st
import pandas as pd
import plotly.express as px
import os
import logging
import time
from controlflow import Agent, Task as CFTask
from pydantic import BaseModel
from prefect import task, flow
from prefect.futures import PrefectFuture
from prefect.logging import get_run_logger
from typing import Dict
from prefect.artifacts import (
    create_progress_artifact,
    update_progress_artifact,
    create_link_artifact,
    create_markdown_artifact,
    create_table_artifact,
)
# Configure logging
logging.basicConfig(level=logging.INFO)

# Dictionary to store execution times for each task
execution_times = {}

# Define the AI Agent for report generation
audit_agent = Agent(
    name="Auditor",
    model="gpt-3.5-turbo",
    description="An AI agent that is an expert in auditing, data analysis, and writing reports.",
    instructions="""
        Your primary goal is to validate the data, generate an audit report, and ensure that the data
        is ready for analysis. You will be working with the data provided to you,
        and you will need to validate it and generate a report one paragraph long.
    """,
)

# Define the data model for the audit report
class AuditReport(BaseModel):
    title: str
    markdown: str

# Task to load the sales data
@task(name="load_data", retries=3, retry_delay_seconds=10, log_prints=True)
def load_data(uploaded_file) -> pd.DataFrame:
    """
    Load the sales data from a CSV file.
    """
    if uploaded_file is not None:
        try:
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file)
            print(f"DataFrame shape: {df.shape}")
            print(f"DataFrame columns: {df.columns}")
            return df
        except Exception as e:
            print(f"Error loading data: {str(e)}")
            raise
    else:
        return pd.DataFrame()

# Task to check the data for any issues
@task(name="check_data", retries=2, retry_delay_seconds=5, log_prints=True)
def check_data(df: pd.DataFrame) -> bool:
    """
    Check the data for any issues.
    """
    required_columns = ["Total Revenue", "Units Sold", "Product Category", "Region", "Payment Method"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        return False
    if df.empty:
        return False
    return True

# Task to compute overview metrics
@task(name="compute_overview", log_prints=True)
def compute_overview(df: pd.DataFrame) -> Dict[str, float]:
    """
    Compute overview metrics.
    """
    start_time = time.time()
    total_transactions = len(df)
    total_revenue = df['Total Revenue'].sum()
    average_order_value = df['Total Revenue'].mean()
    duration = time.time() - start_time

    return {
      "metrics": {
            "total_transactions": total_transactions,
            "total_revenue": total_revenue,
            "average_order_value": average_order_value
        },
        "execution_time": duration
    }

# Visualization tasks, each dependent on the previous one
@task(name="plot_category_sales", log_prints=True)
def plot_category_sales(df: pd.DataFrame) -> px.bar:
    start_time = time.time()

    category_sales = df.groupby('Product Category')['Total Revenue'].sum().sort_values(ascending=False)
    fig_category = px.bar(
        category_sales.reset_index(),
        x='Product Category',
        y='Total Revenue',
        title='Total Revenue by Product Category'
    )
    duration = time.time() - start_time
    return {
        "figure": fig_category,
        "execution_time": duration
    }

@task(name="plot_sales_trend", log_prints=True)
def plot_sales_trend(df: pd.DataFrame) -> px.line:
    start_time = time.time()
    df['Date'] = pd.to_datetime(df['Date'])
    daily_sales = df.groupby('Date')['Total Revenue'].sum().reset_index()
    fig_trend = px.line(
        daily_sales,
        x='Date',
        y='Total Revenue',
        title='Daily Sales Trend'
    )
    duration = time.time() - start_time
    return {
        "figure": fig_trend,
        "execution_time": duration
    }

@task(name="plot_top_products", log_prints=True)
def plot_top_products(df: pd.DataFrame) -> px.bar:
    start_time = time.time()
    top_products = df.groupby('Product Name')['Units Sold'].sum().sort_values(ascending=False).head(10).reset_index()
    fig_products = px.bar(
        top_products,
        x='Product Name',
        y='Units Sold',
        title='Top 10 Selling Products'
    )
    duration = time.time() - start_time
    return {
        "figure": fig_products,
        "execution_time": duration
    }

@task(name="plot_regional_sales")
def plot_regional_sales(df: pd.DataFrame) -> px.pie:
    start_time = time.time()
    region_sales = df.groupby('Region')['Total Revenue'].sum().reset_index()
    fig_region = px.pie(
        region_sales,
        values='Total Revenue',
        names='Region',
        title='Sales Distribution by Region'
    )
    duration = time.time() - start_time
    return {
        "figure": fig_region,
        "execution_time": duration
    }

@task(name="plot_payment_methods")
def plot_payment_methods(df: pd.DataFrame) -> px.pie:
    start_time = time.time()
    payment_method = df['Payment Method'].value_counts().reset_index()
    payment_method.columns = ['Payment Method', 'Count']
    fig_payment = px.pie(
        payment_method,
        values='Count',
        names='Payment Method',
        title='Payment Method Distribution'
    )
    duration = time.time() - start_time
    return {
        "figure": fig_payment,
        "execution_time": duration
    }

def create_execution_times_markdown(task_execution_times: list):
    """
    Creates a markdown table of task execution times and creates a markdown artifact.
    """
    # Generate markdown table
    markdown = "# Task Execution Times\n\n"
    markdown += "| Task | Execution Time (seconds) |\n"
    markdown += "|------|--------------------------|\n"
    total_time = 0
    for task_info in task_execution_times:
        task_name = task_info["Task"]
        execution_time = task_info["Execution Time (s)"]
        total_time += execution_time
        markdown += f"| {task_name} | {execution_time:.2f} |\n"
    markdown += f"\n**Total Execution Time:** {total_time:.2f} seconds\n"
    
    # Create markdown artifact
    create_markdown_artifact(
        key="execution-times",
        markdown=markdown,
        description="Execution times of each task in the data analysis flow.",
    )
    
@flow(name="control_flow_analysis")
def control_flow_analysis(df: pd.DataFrame):
    """
    Analyze the sales data using ControlFlow.
    """
    # Create a specialized agent 
    classifier = cf.Agent(
        name="Email Classifier",
        model="openai/gpt-4o-mini",
        instructions="You are an expert at quickly classifying emails.",
    )

    # Initialize a list to collect execution times
    emails = [
        "Hello, I need an update on the project status.",
        "Subject: Exclusive offer just for you!",
        "Urgent: Project deadline moved up by one week.",
    ]
    time.sleep(3)
    # Set up a ControlFlow task to classify emails
    classifications = cf.run(
        'Classify the emails',
        result_type=['important', 'spam'],
        agents=[classifier],
        context=dict(emails=emails),
    )
    # Check the data
    # Create a ControlFlow task to generate an reply
    reply = cf.run(
        "Write a polite reply to an email",
        context=dict(email=emails[0]),
    )
    logger.info(f"Classifications: {classifications}")
    logger.info(f"Reply: {reply}")

    

# Main analysis flow
@flow(name="analyze_data", log_prints=True, description="Analyze the sales data using Prefect tasks and flows.")
def analyze_data(df: pd.DataFrame):
    """
    Analyze the sales data using Prefect tasks and flows.
    """
    logger = get_run_logger()
    logger.info("Starting data analysis flow")

    # Initialize a list to collect execution times
    task_execution_times = []
    # Check the data
    logger.info("Checking data validity")
    data_valid = check_data.submit(df)
    if not data_valid.result():
        logger.error("Data validation failed")
        st.error("Data validation failed. Please check your data and try again.")
        return

    # Compute overview metrics
    logger.info("Computing overview metrics")
    overview = compute_overview.submit(df)
    overview_result = overview.result()
    overview_metrics = overview_result["metrics"]
    overview_execution_time = overview_result["execution_time"]
    task_execution_times.append({
        "Task": "Compute Overview",
        "Execution Time (s)": overview_execution_time
    })

    # Display overview
    st.header('Overview')
    st.write(f"Total Transactions: {overview_metrics['total_transactions']}")
    st.write(f"Total Revenue: ${overview_metrics['total_revenue']:,.2f}")
    st.write(f"Average Order Value: ${overview_metrics['average_order_value']:,.2f}")

    # Create a table artifact for overview metrics
    overview_df = pd.DataFrame([overview_metrics])
    # Create a table artifact for overview metrics
    logger.info("Creating table artifact for overview metrics")
    create_table_artifact(
        key="overview-metrics",
        table=[overview_metrics],  # Wrap the dictionary in a list
        description="Overview metrics of the sales data analysis.",
    )

    # Initialize progress bar and artifact
    progress_bar = st.progress(0)
    progress = 0
    logger.info("Creating progress artifact")
    progress_artifact_id = create_progress_artifact(
        progress=progress / 100,  # Convert to 0.0 - 1.0
        description="Data analysis started.",
    )

    # Visualization tasks with dependencies
    logger.info("Plotting category sales")
    fig_category_future = plot_category_sales.submit(df)
    fig_category_result = fig_category_future.result()
    fig_category = fig_category_result["figure"]
    fig_category_execution_time = fig_category_result["execution_time"]
    task_execution_times.append({
        "Task": "Plot Category Sales",
        "Execution Time (s)": fig_category_execution_time
    })
    progress += 20
    progress_bar.progress(progress)
    update_progress_artifact(
        progress=progress / 100,
        artifact_id=progress_artifact_id,
        description="Category sales analysis completed.",
    )
    st.plotly_chart(fig_category)
    time.sleep(4)

    # Continue similarly for other visualization tasks...
    # Plot Sales Trend
    logger.info("Plotting sales trend")
    fig_trend_future = plot_sales_trend.submit(df, wait_for=[fig_category_future])
    fig_trend_result = fig_trend_future.result()
    fig_trend = fig_trend_result["figure"]
    fig_trend_execution_time = fig_trend_result["execution_time"]
    task_execution_times.append({
        "Task": "Plot Sales Trend",
        "Execution Time (s)": fig_trend_execution_time
    })
    progress += 20
    progress_bar.progress(progress)
    update_progress_artifact(
        progress=progress / 100,
        artifact_id=progress_artifact_id,
        description="Sales trend analysis completed.",
    )
    st.plotly_chart(fig_trend)
    time.sleep(8)

    # Plot Top Products
    logger.info("Plotting top products")
    fig_products_future = plot_top_products.submit(df, wait_for=[fig_trend_future])
    fig_products_result = fig_products_future.result()
    fig_products = fig_products_result["figure"]
    fig_products_execution_time = fig_products_result["execution_time"]
    task_execution_times.append({
        "Task": "Plot Top Products",
        "Execution Time (s)": fig_products_execution_time
    })
    progress += 20
    progress_bar.progress(progress)
    update_progress_artifact(
        progress=progress / 100,
        artifact_id=progress_artifact_id,
        description="Top products analysis completed.",
    )
    st.plotly_chart(fig_products)
    time.sleep(5)

    # Plot Regional Sales
    logger.info("Plotting regional sales")
    fig_region_future = plot_regional_sales.submit(df, wait_for=[fig_products_future])
    fig_region_result = fig_region_future.result()
    fig_region = fig_region_result["figure"]
    fig_region_execution_time = fig_region_result["execution_time"]
    task_execution_times.append({
        "Task": "Plot Regional Sales",
        "Execution Time (s)": fig_region_execution_time
    })
    progress += 20
    progress_bar.progress(progress)
    update_progress_artifact(
        progress=progress / 100,
        artifact_id=progress_artifact_id,
        description="Regional sales analysis completed.",
    )
    st.plotly_chart(fig_region)
    time.sleep(7)

    # Plot Payment Methods
    logger.info("Plotting payment methods")
    fig_payment_future = plot_payment_methods.submit(df, wait_for=[fig_region_future])
    fig_payment_result = fig_payment_future.result()
    fig_payment = fig_payment_result["figure"]
    fig_payment_execution_time = fig_payment_result["execution_time"]
    task_execution_times.append({
        "Task": "Plot Payment Methods",
        "Execution Time (s)": fig_payment_execution_time
    })
    progress += 10
    progress_bar.progress(progress)
    update_progress_artifact(
        progress=progress / 100,
        artifact_id=progress_artifact_id,
        description="Payment methods analysis completed.",
    )
    st.plotly_chart(fig_payment)
    time.sleep(8)

    # Display interactive explorer
    logger.info("Displaying interactive data explorer")
    start_time = time.time()
    st.header('Interactive Data Explorer')
    selected_columns = st.multiselect(
        'Select columns to display',
        df.columns.tolist(),
        default=['Transaction ID', 'Date', 'Product Name', 'Total Revenue']
    )
    st.dataframe(df[selected_columns])
    interactive_explorer_execution_time = time.time() - start_time
    task_execution_times.append({
        "Task": "Interactive Data Explorer",
        "Execution Time (s)": interactive_explorer_execution_time
    })

    progress += 10
    progress_bar.progress(progress)
    update_progress_artifact(
        progress=progress / 100,
        artifact_id=progress_artifact_id,
        description="Interactive explorer displayed.",
    )

    # Generate audit report outside of Prefect tasks
    logger.info("Generating audit report")
    start_time = time.time()
    

    audit_report_execution_time = time.time() - start_time
    task_execution_times.append({
        "Task": "Generate Audit Report",
        "Execution Time (s)": audit_report_execution_time
    })

    # Final progress update
    progress = 100
    progress_bar.progress(progress)
    update_progress_artifact(
        progress=progress / 100,
        artifact_id=progress_artifact_id,
        description="Data analysis completed successfully.",
    )
    st.success("Data analysis completed successfully!")
    # Create markdown table of execution times
    logger.info("Creating execution times markdown")
    create_execution_times_markdown(task_execution_times)

    logger.info("Data analysis flow completed")
    my_flow()

@task
def always_fails_task():
    time.sleep(5)
    raise ValueError("I am bad task")


@task
def always_succeeds_task():
    time.sleep(5)
    return "foo"


@flow
def always_succeeds_flow():
    time.sleep(5)
    return "bar"


@flow
def always_fails_flow():
    x = always_fails_task()
    y = always_succeeds_task()
    z = always_succeeds_flow()
    return x, y, z


@task 
def add_one(x):
    return x + 1

@flow 
def my_flow():
    # avoided raising an exception via `return_state=True`
    state = add_one("1", return_state=True)
    assert state.is_failed()
    always_fails_flow()
    time.sleep(20)



# Main function to run the Streamlit app
@flow(name="sales_analysis_flow", log_prints=True,)
def main():
    """
    Main function to run the Streamlit app.
    """
    st.title('Online Sales Data Analysis')
    st.write("This app allows you to analyze online sales data. You can upload a CSV file containing the sales data.")
    uploaded_file = st.file_uploader("Choose a CSV file", type=['csv', 'xlsx', 'xls'])
    if uploaded_file is not None:
        with st.spinner('Loading data...'):
            try:
                df_future = load_data.submit(uploaded_file)
                time.sleep(7)
                df = df_future.result()
                if not df.empty:
                    st.success('Data loaded successfully!')
                    try:
                        analyze_data(df)
                    except Exception as e:
                        st.error(f"An error occurred during data analysis: {e}")
                        logging.error("Error during data analysis", exc_info=True)
                    try:
                        my_flow()
                    except Exception as e:
                        st.error(f"An error occurred in my_flow: {e}")
                        logging.error("Error in my_flow", exc_info=True)
                else:
                    st.error("No data found in the uploaded file.")
            except Exception as e:
                st.error(f"An error occurred while loading the data: {e}")
                logging.error("Error during data loading", exc_info=True)
    else:
        st.info("Please upload a CSV file to proceed.")

        
    # Check the Prefect state of the analyze_data function
    # load_data_state = load_data.submit
    # time.sleep(5)
    # if load_data_state and load_data_state.result():
    #     st.write("Data analysis completed. Running additional control flow analysis...")
    #     # Run the control flow analysis
    #     control_flow_analysis(df)
        
    # else:
    #     st.write(f"Data analysis not yet complete. Current state: {load_data_state.name if load_data_state else 'Unknown'}")

if __name__ == "__main__":    
    main()