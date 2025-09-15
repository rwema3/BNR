# BNR CHAllenge

This repository contains scripts and instructions to **ingest Amazon product reviews into ClickHouse**, perform **data analysis**, and visualize insights. The project uses **Docker**, **ClickHouse**, and **Python 3.10** with Polars for high-performance data handling.

---

## Table of Contents

1. [Project Overview](#project-overview)  
2. [Assumptions](#assumptions)  
3. [Environment Setup](#environment-setup)  
    - [Install Docker](#install-docker)  
    - [Docker Compose for ClickHouse](#docker-compose-for-clickhouse)  
    - [Python Virtual Environment](#python-virtual-environment)  
    - [Install Utilities](#install-utilities)  
4. [Data Ingestion](#data-ingestion)  
    - [Configure Environment Variables](#configure-environment-variables)  
    - [Run Ingestion Script](#run-ingestion-script)  
5. [Analysis & Visualization](#analysis--visualization)  
6. [Reproducing Results](#reproducing-results)  
7. [License](#license)  

---

## Project Overview

This project performs the following tasks:

1. **Data ingestion:** Reads JSON Lines (`.jsonl`) from Amazon product review datasets and inserts them into a ClickHouse table.  
2. **Database setup:** Uses ClickHouse with `ReplacingMergeTree` for deduplication support.  
3. **Analysis & Visualization:** Generates insights such as top products, rating distributions, verified purchase percentages, and top reviewers.  

---

## Assumptions

- Ubuntu 20.04+ environment.  
- Docker and Docker Compose installed.  
- Python 3.10 installed.  
- Access to an AWS server to execute the project.  
- Dataset downloaded from the [UCSD Amazon Product Reviews Repository](https://nijianmo.github.io/amazon/index.html) (e.g., `Subscription_Boxes.jsonl`).  
- Credentials for ClickHouse are stored in a `.env` file for security.  

---

## Environment Setup

### Install Docker

If Docker is not installed:

```bash
# Update package index
sudo apt update

# Install Docker dependencies
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common

# Add Docker GPG key
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# Add Docker repository
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker and Docker Compose
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Verify installation
docker --version
docker compose version
