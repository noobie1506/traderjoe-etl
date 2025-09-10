# Trader Joe ETL Pipeline

An automated **ETL (Extract, Transform, Load) pipeline** that collects, processes, and analyzes **DeFi liquidity pool data** from the [Trader Joe Liquidity Pools](https://lfj.gg/avalanche/pool) on the Avalanche blockchain.  

This project demonstrates skills in **Python, APIs, Web3, and data engineering** by combining off-chain API data (from the **LFJ DEX API v1.0.0**) with on-chain smart contract queries (via **helperContractABI.json**) to calculate key liquidity metrics and save results into CSVs for further analysis.

---

## 🚀 Features

- **Extract**  
  - Fetches pool data (volume, liquidity, fees) from the **LFJ DEX API (v1.0.0)**  
  - Pulls user deposit/withdraw history and accrued fees  
  - Queries on-chain reserves directly from Avalanche smart contracts using **Web3** and `helperContractABI.json`  

- **Transform**  
  - Normalizes JSON into clean tabular data with `pandas`  
  - Aggregates fees and deposits by most recent transaction  
  - Calculates custom metrics:  
    - APR / APY  
    - Impermanent loss  
    - User’s % share of pool liquidity  

- **Load**  
  - Saves results into a structured CSV (`merged_data.csv`)  
  - Automatically appends new hourly snapshots without duplicating headers

