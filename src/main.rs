use byteorder::{BigEndian, ReadBytesExt};
use flate2::read::GzDecoder;
use polars::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::Cursor;
use std::io::{self, BufReader, Read};
use std::time::Instant;

const NS_PER_HOUR: u64 = 60 * 60 * 1_000_000_000;

struct CumulativeData {
    cum_price_qty: u128, // Cumulative (Price x Quantity)
    cum_qty: u128,       // Cumulative Quantity
}

fn msg_size(msg_type: u8) -> usize {
    match msg_type {
        b'S' => 11,
        b'R' => 38,
        b'H' => 24,
        b'Y' => 19,
        b'L' => 25,
        b'V' => 34,
        b'W' => 11,
        b'K' => 27,
        b'J' => 34,
        b'h' => 20,
        b'A' => 35,
        b'F' => 39,
        b'E' => 30,
        b'C' => 35,
        b'X' => 22,
        b'D' => 18,
        b'U' => 34,
        b'P' => 43,
        b'Q' => 39,
        b'B' => 18,
        b'I' => 49,
        b'N' => 19,
        _ => 0,
    }
}

fn get_hour(timestamp: &[u8; 6]) -> u8 {
    // Prepend two zero bytes before reading as u64.
    let mut ns: [u8; 8] = [0u8; 8];
    ns[2..].copy_from_slice(timestamp);
    let ns: u64 = Cursor::new(&ns).read_u64::<BigEndian>().unwrap();
    return (ns / NS_PER_HOUR) as u8;
}

fn parse_trade(msg: &[u8]) -> (u8, Vec<u8>, u128, u128) {
    let mut rdr: Cursor<&[u8]> = Cursor::new(msg);

    // Stock Locate (2 Bytes)
    let _: u16 = rdr.read_u16::<BigEndian>().unwrap();

    // Tracking Number (2 Bytes)
    let _: u16 = rdr.read_u16::<BigEndian>().unwrap();

    // Timestamp (6 Bytes)
    let mut timestamp: [u8; 6] = [0u8; 6];
    rdr.read_exact(&mut timestamp).unwrap();
    let hour: u8 = get_hour(&timestamp);

    // Order Reference Number (8 Bytes)
    let _ = rdr.read_u64::<BigEndian>().unwrap();

    // Buy/Sell Indicator (1 Byte)
    let _ = rdr.read_u8().unwrap();

    // Shares (4 Bytes)
    let qty: u32 = rdr.read_u32::<BigEndian>().unwrap();

    // Stock (8 Bytes)
    let mut stock: Vec<u8> = vec![0u8; 8];
    rdr.read_exact(&mut stock).unwrap();

    // Price (4 Bytes)
    let price: u32 = rdr.read_u32::<BigEndian>().unwrap();

    return (hour as u8, stock, price as u128, qty as u128);
}

fn process_file(filename: &str) -> io::Result<()> {
    let file: File = File::open(filename)?;

    // Initialize HashMap to store CumulativeData for each hour/stock pair.
    let mut data: HashMap<(u8, Vec<u8>), CumulativeData> = HashMap::new();

    let mut msg_type: [u8; 1] = [0u8; 1];
    let mut rdr: BufReader<GzDecoder<File>> = BufReader::new(GzDecoder::new(file));
    while rdr.read_exact(&mut msg_type).is_ok() {
        let size: usize = msg_size(msg_type[0]);
        let mut msg: Vec<u8> = vec![0u8; size];
        rdr.read_exact(&mut msg)?;

        if msg_type[0] == b'P' {
            // Parse trade message and cumulate data.
            let (hour, stock, price, qty) = parse_trade(&msg);
            let entry: &mut CumulativeData =
                data.entry((hour, stock.clone()))
                    .or_insert_with(|| CumulativeData {
                        cum_price_qty: 0,
                        cum_qty: 0,
                    });
            entry.cum_price_qty += price * qty;
            entry.cum_qty += qty;
        }
    }

    let size = data.len();
    let (mut hours, mut stocks, mut vwaps) = (
        Vec::with_capacity(size),
        Vec::with_capacity(size),
        Vec::with_capacity(size),
    );
    for ((hour, stock), d) in data {
        hours.push(hour as u32);

        // Cast stock ticker from bytes to string and strip padding.
        stocks.push(String::from_utf8_lossy(&stock).trim_end().to_string());

        // Compute volume-weighted average price and adjust scale.
        let unscaled_vwap = (d.cum_price_qty as f64) / (d.cum_qty as f64);
        vwaps.push(unscaled_vwap / 10_000.0);
    }

    // Build dataframe and write to parquet.
    let mut df = df!("hour" => hours, "stock" => stocks, "vwap" => vwaps)
        .unwrap()
        .sort(["hour", "stock"], SortMultipleOptions::default())
        .unwrap();
    let output_filename = "vwap.parquet";
    let mut file = File::create(output_filename).unwrap();
    ParquetWriter::new(&mut file).finish(&mut df).unwrap();
    Ok(())
}

fn main() {
    let filename: String = std::env::args()
        .nth(1)
        .unwrap_or("01302019.NASDAQ_ITCH50.gz".to_string());

    let start: Instant = Instant::now();
    if let Err(e) = process_file(&filename) {
        eprintln!("Error processing file: {}", e);
    } else {
        println!("Computed VWAP in {:.2?}.", start.elapsed());
    }
}
