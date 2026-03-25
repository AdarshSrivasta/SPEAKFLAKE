# SPEAKFLAKE Deployment Guide

## Prerequisites

- Snowflake account with ACCOUNTADMIN (or equivalent) privileges
- Cortex COMPLETE and Cortex Analyst enabled in your region
- A warehouse (X-Small is sufficient for demo)

## Step-by-Step Deployment

### 1. Create Database & Schemas

```sql
-- Run sql/01_setup_database.sql
