import streamlit as st
import pandas as pd
import boto3
import io

# MinIO Configuration
minio_endpoint = 'http://localhost:9000'
minio_access_key = 'minioadmin'
minio_secret_key = 'minioadmin'
analytics_bucket = 'analytics'

# Connect to MinIO
s3_client = boto3.client(
    's3',
    endpoint_url=minio_endpoint,
    aws_access_key_id=minio_access_key,
    aws_secret_access_key=minio_secret_key
)

# Function to Read Parquet Files from MinIO
def read_parquet_from_minio(bucket_name, key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        return pd.read_parquet(io.BytesIO(response['Body'].read()))
    except Exception as e:
        st.error(f"Error reading {key} from bucket {bucket_name}: {e}")
        return None

# Streamlit Dashboard
st.title("Chinook Analytics Dashboard")

# Load Customer Sales Summary
st.subheader("Customer Sales Summary")
customer_data = read_parquet_from_minio(analytics_bucket, "customer_sales_summary.parquet")
if customer_data is not None:
    st.dataframe(customer_data)
    st.bar_chart(customer_data.set_index("CustomerId")["TotalSales"].sort_values(ascending=False).head(10))

# Load Monthly Sales Trends
st.subheader("Monthly Sales Trends")
monthly_data = read_parquet_from_minio(analytics_bucket, "monthly_sales_trends.parquet")
if monthly_data is not None:
    st.dataframe(monthly_data)
    monthly_data["YearMonth"] = monthly_data["YearMonth"].astype(str)
    st.line_chart(monthly_data.set_index("YearMonth")["TotalSales"])
