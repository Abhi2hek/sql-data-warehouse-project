#  Data Warehouse Project

This project demonstrates the design and implementation of a *Data Warehouse* using a layered architecture (*Bronze, Silver, Gold*) to store, clean, transform, and analyze business data.  
It focuses on *ETL processes, SQL transformations, and analytical querying* for reporting and insights.



##  Project Objectives

- Collect raw data from multiple sources
- Clean and standardize data
- Transform data for analytical use
- Enable fast reporting and business intelligence
- Implement SQL-based ETL 


##  Architecture

## Bronze Layer (Raw Data)
- Stores raw, unprocessed data
- Data loaded as-is from source systems
- Used for auditing and traceability

## Silver Layer (Cleaned Data)
- Removes duplicates and null values
- Data type corrections
- Standardized formats

##  Gold Layer (Business-Ready Data)
- Aggregated and analytical tables
- Optimized for reporting
- Used by BI tools and dashboards



##  Technologies Used


- SQL Server 
- ETL Concepts
- Data Modeling (Star Schema)
- Stored Procedures
- Triggers
- View
