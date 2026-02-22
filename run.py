import pandas as pd
import numpy as np
import yaml
import json
import argparse
import time
import logging
import os
import sys

def setup_logging(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

def main():
    start_time = time.time()
    
    # CLI Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)
    args = parser.parse_args()

    setup_logging(args.log_file)
    logging.info("Job started")

    metrics = {"status": "error", "version": "unknown"}

    try:
        # 1. Load Config
        if not os.path.exists(args.config):
            raise FileNotFoundError(f"Config file {args.config} not found")
        
        with open(args.config, 'r') as f:
            conf = yaml.safe_load(f)
        
        seed = conf.get('seed')
        window = conf.get('window')
        version = conf.get('version')
        metrics["version"] = version
        metrics["seed"] = seed

        if None in [seed, window, version]:
            raise ValueError("Invalid configuration file structure")

        np.random.seed(seed)
        logging.info(f"Config loaded: seed={seed}, window={window}, version={version}")

        # 2. Ingest Data
        if not os.path.exists(args.input):
            raise FileNotFoundError(f"Input file {args.input} not found")
        
        df = pd.read_csv(args.input)
        if df.empty:
            raise ValueError("Empty input file")
        if 'close' not in df.columns:
            raise ValueError("Missing required 'close' column")
        
        rows_processed = len(df)
        logging.info(f"Data loaded: {rows_processed} rows")

        # 3. Logic: Rolling Mean & Signals
        # Note: min_periods=window ensures NaN for initial rows where data < window
        df['rolling_mean'] = df['close'].rolling(window=window).mean()
        logging.info(f"Rolling mean calculated with window={window}")

        # Signal Logic: 1 if close > rolling_mean, 0 otherwise
        # Handling NaNs: if rolling_mean is NaN (initial rows), signal is 0 per comparison logic
        df['signal'] = (df['close'] > df['rolling_mean']).astype(int)
        logging.info("Signals generated")

        # 4. Metrics
        signal_rate = float(df['signal'].mean())
        end_time = time.time()
        latency_ms = int((end_time - start_time) * 1000)

        metrics.update({
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(signal_rate, 4),
            "latency_ms": latency_ms,
            "status": "success"
        })

        logging.info(f"Metrics: signal_rate={metrics['value']}, rows_processed={rows_processed}")
        logging.info(f"Job completed successfully in {latency_ms}ms")

    except Exception as e:
        error_msg = str(e)
        logging.error(f"Error encountered: {error_msg}")
        metrics["status"] = "error"
        metrics["error_message"] = error_msg

    # Write JSON Output
    with open(args.output, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    # Print to stdout for Docker visibility
    print(json.dumps(metrics, indent=2))
    
    if metrics["status"] == "error":
        sys.exit(1)

if __name__ == "__main__":
    main()
