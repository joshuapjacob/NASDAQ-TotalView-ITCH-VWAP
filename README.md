# NASDAQ TotalView-ITCH VWAP

A Rust program to compute volume-weighted average price (VWAP) for each stock at every hour from a [NASDAQ TotalView-ITCH 5.0](https://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHspecification.pdf) tick data file.

## Usage

```shell
# Download a sample data file (around 5 GB).
curl -O https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/01302019.NASDAQ_ITCH50.gz

# Compute the VWAP.
cargo run --release 01302019.NASDAQ_ITCH50.gz
```

Output is written to a `vwap.parquet` file in the current directory.
