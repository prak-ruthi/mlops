# MLOps Engineering Internship: Technical Assessment
## Project: MetaStackerBandit Signal Pipeline

This repository contains a miniature MLOps-style pipeline designed for reproducibility, structured logging, and containerized deployment. The application processes cryptocurrency OHLCV data to generate trading signals based on rolling mean crossovers.

---

## 1. Project Structure
- `run.py`: Main Python script for data processing and signal generation.
- `config.yaml`: Configuration file for hyperparameters (seed, window, version).
- `data.csv`: Source dataset containing 10,000 rows of OHLCV data.
- `requirements.txt`: Python dependencies.
- `Dockerfile`: Container definition for automated execution.
- `metrics.json`: Example output of calculated metrics.
- `run.log`: Example execution log.

---

## 2. Setup Instructions

### Prerequisites
- Python 3.9+
- Docker (optional, for containerized execution)

### Install Dependencies
To run the script locally, install the required libraries:
```bash
pip install -r requirements.txt
