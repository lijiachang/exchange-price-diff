use std::collections::HashMap;
use std::fs;
use std::fs::{metadata, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::string::ToString;
use std::sync::Arc;
use std::time::SystemTime;
use chrono::Local;
use crypto_markets::fetch_markets;
use crypto_crawler::{crawl_l2_topk, crawl_trade, MarketType};
use crypto_markets::Market;
use serde::{Deserialize, Serialize};

static BINANCE: &str = "BINANCE";
static GATE: &str = "GATE";

#[derive(Debug, Deserialize)]
struct Config {
    symbols: Vec<String>,
}


const TRADE_CSV_HEADERS: &[&str] = &[
    "timestamp",
    "local_timestamp",
    "exchange",
    "symbol",
    "side",
    "price",
    "qty",
];

const ORDERBOOK_CSV_HEADERS: &[&str] = &[
    "timestamp",
    "local_timestamp",
    "exchange",
    "symbol",
    "ask_1_price",
    "ask_1_qty",
    "ask_2_price",
    "ask_2_qty",
    "ask_3_price",
    "ask_3_qty",
    "ask_4_price",
    "ask_4_qty",
    "ask_5_price",
    "ask_5_qty",
    "ask_6_price",
    "ask_6_qty",
    "ask_7_price",
    "ask_7_qty",
    "ask_8_price",
    "ask_8_qty",
    "ask_9_price",
    "ask_9_qty",
    "ask_10_price",
    "ask_10_qty",
    "bid_1_price",
    "bid_1_qty",
    "bid_2_price",
    "bid_2_qty",
    "bid_3_price",
    "bid_3_qty",
    "bid_4_price",
    "bid_4_qty",
    "bid_5_price",
    "bid_5_qty",
    "bid_6_price",
    "bid_6_qty",
    "bid_7_price",
    "bid_7_qty",
    "bid_8_price",
    "bid_8_qty",
    "bid_9_price",
    "bid_9_qty",
    "bid_10_price",
    "bid_10_qty",
];

#[derive(Debug, Serialize, Deserialize)]
struct OrderbookMsg {
    timestamp: i64,
    local_timestamp: i64,
    exchange: String,
    symbol: String,
    asks: Vec<(f64, f64)>,
    bids: Vec<(f64, f64)>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TradeMsg {
    timestamp: i64,
    local_timestamp: i64,
    exchange: String,
    symbol: String,
    side: String,
    price: f64,
    qty: f64,
}

fn round_to_precision(num: f64, precision: f64) -> f64 {
    let multiple = 1.0 / precision;
    (num * multiple).round() / multiple
}

fn write_orderbook_csv(file_path: &str, data: &[OrderbookMsg]) {
    //data/exchange_name/symbol_name/orderbook/date.csv
    // 创建目录（如果不存在）
    let dir_path = Path::new(file_path).parent().unwrap();
    fs::create_dir_all(dir_path).unwrap();

    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(file_path)
        .unwrap();

    // 检查文件是否已经存在且不为空, 写入表头
    let need_write_headers = match metadata(file_path) {
        Ok(meta) => meta.len() == 0,
        Err(_) => true,
    };
    if need_write_headers {
        writeln!(file, "{}", ORDERBOOK_CSV_HEADERS.join(",")).unwrap();
    }

    for msg in data {
        let mut row = vec![
            msg.timestamp.to_string(),
            msg.local_timestamp.to_string(),
            msg.exchange.to_string(),
            msg.symbol.to_string(),
        ];
        for (price, qty) in msg.asks.iter().take(10) {
            row.push(price.to_string());
            row.push(qty.to_string());
        }
        for (price, qty) in msg.bids.iter().take(10) {
            row.push(price.to_string());
            row.push(qty.to_string());
        }
        writeln!(file, "{}", row.join(",")).unwrap();
    }
    println!("{} Write {} records to {}", Local::now().format("%Y-%m-%d %H:%M:%S"), data.len(), file_path);
}

fn write_trade_csv(file_path: &str, data: &[TradeMsg]) {
    let dir_path = Path::new(file_path).parent().unwrap();
    fs::create_dir_all(dir_path).unwrap();

    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(file_path)
        .unwrap();

    // 检查文件是否已经存在且不为空, 写入表头
    let need_write_headers = match metadata(file_path) {
        Ok(meta) => meta.len() == 0,
        Err(_) => true,
    };
    if need_write_headers {
        writeln!(file, "{}", TRADE_CSV_HEADERS.join(",")).unwrap();
    }


    for msg in data {
        let row = vec![
            msg.timestamp.to_string(),
            msg.local_timestamp.to_string(),
            msg.exchange.clone(),
            msg.symbol.clone(),
            msg.side.clone(),
            msg.price.to_string(),
            msg.qty.to_string(),
        ];
        writeln!(file, "{}", row.join(",")).unwrap();
    }
    println!("{} Write {} records to {}", Local::now().format("%Y-%m-%d %H:%M:%S"), data.len(), file_path);
}

/// 获取Gate合约信息（合约乘数）
async fn get_gate_markets() -> Arc<HashMap<String, Market>> {
    let markets = tokio::task::spawn_blocking(|| { fetch_markets("gate", MarketType::LinearSwap).unwrap() })
        .await
        .unwrap();

    println!("get_gate_markets len={}", &markets.len());

    let markets_map = markets.into_iter()
        .map(|m| (m.symbol.clone(), m))
        .collect::<HashMap<String, Market>>();

    Arc::new(markets_map)
}

#[tokio::main]
async fn main() {
    // unbounded channel
    let (binance_orderbook_tx, binance_orderbook_rx) = std::sync::mpsc::channel();
    let (gate_orderbook_tx, gate_orderbook_rx) = std::sync::mpsc::channel();
    let (binance_trade_tx, binance_trade_rx) = std::sync::mpsc::channel();
    let (gate_trade_tx, gate_trade_rx) = std::sync::mpsc::channel();


    let config_str = fs::read_to_string("config.toml").expect("Failed to read config file");
    let config: Config = toml::from_str(&config_str).expect("Failed to parse config file");

    let binance_symbols: Arc<Vec<String>> = Arc::new(config.symbols.iter().map(|x| x.replace("_", "").to_string()).collect());
    println!("binance_symbols: {:?}", &binance_symbols);
    let gate_symbols: Arc<Vec<String>> = Arc::new(config.symbols.iter().map(|x| x.to_string()).collect());
    println!("gate_symbols: {:?}", &gate_symbols);

    // 合约信息map
    let gate_markets = get_gate_markets().await;


    // Binance orderbook task
    let symbols_clone = binance_symbols.clone();
    tokio::spawn(async move {
        crawl_l2_topk("binance", MarketType::LinearSwap, Some(&symbols_clone), binance_orderbook_tx).await;
    });

    // Gate orderbook task
    let symbols_clone = gate_symbols.clone();
    tokio::spawn(async move {
        crawl_l2_topk("gate", MarketType::LinearSwap, Some(&symbols_clone), gate_orderbook_tx).await;
    });

    // Binance trade task
    let symbols_clone = binance_symbols.clone();
    tokio::spawn(async move {
        crawl_trade("binance", MarketType::LinearSwap, Some(&symbols_clone), binance_trade_tx).await;
    });

    // Gate trade task
    let symbols_clone = gate_symbols.clone();
    tokio::spawn(async move {
        crawl_trade("gate", MarketType::LinearSwap, Some(&symbols_clone), gate_trade_tx).await;
    });

    // Consume messages and write to CSV files
    tokio::spawn({
        let binance_symbols = binance_symbols.clone();
        async move {
            let mut symbol_orderbooks: HashMap<String, Vec<OrderbookMsg>> = binance_symbols.iter()
                .map(|x| (x.to_string(), vec![]))
                .collect();

            while let Ok(msg) = binance_orderbook_rx.recv() {
                let local_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as i64;
                let parsed_msg: serde_json::Value = serde_json::from_str(&msg.json).unwrap();
                let data = parsed_msg["data"].as_object().unwrap();
                let symbol = data["s"].as_str().unwrap().to_string();
                let timestamp = data["T"].as_i64().unwrap();
                let asks: Vec<(f64, f64)> = data["a"].as_array().unwrap().iter().map(|x| (x[0].as_str().unwrap().parse().unwrap(), x[1].as_str().unwrap().parse().unwrap())).collect();
                let bids: Vec<(f64, f64)> = data["b"].as_array().unwrap().iter().map(|x| (x[0].as_str().unwrap().parse().unwrap(), x[1].as_str().unwrap().parse().unwrap())).collect();
                let orderbook_msg = OrderbookMsg {
                    timestamp,
                    local_timestamp,
                    exchange: BINANCE.to_string(),
                    symbol: symbol.clone(),
                    asks,
                    bids,
                };
                let orderbooks = symbol_orderbooks.get_mut(&symbol).unwrap();
                orderbooks.push(orderbook_msg);

                if orderbooks.len() >= 100 {
                    let date = Local::now().format("%Y-%m-%d");
                    let file_path = format!("data/{BINANCE}/{symbol}/orderbook/{date}.csv");
                    write_orderbook_csv(&*file_path, orderbooks);
                    orderbooks.clear();
                }
            }
        }
    });

    tokio::spawn({
        let gate_symbols = Arc::clone(&gate_symbols);
        let gate_markets = Arc::clone(&gate_markets);
        async move {
            let mut symbol_orderbooks: HashMap<String, Vec<OrderbookMsg>> = gate_symbols.iter()
                .map(|x| (x.to_string(), vec![]))
                .collect();

            while let Ok(msg) = gate_orderbook_rx.recv() {
                let local_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as i64;
                let parsed_msg: serde_json::Value = serde_json::from_str(&msg.json).unwrap();
                let data = parsed_msg["result"].as_object().unwrap();
                let timestamp = data["t"].as_i64().unwrap();
                let contract = data["contract"].as_str().unwrap().to_string();
                let contract_value = gate_markets.get(&contract).unwrap().contract_value.as_ref().unwrap();

                let asks: Vec<(f64, f64)> = data["asks"].as_array().unwrap().iter()
                    .map(|x| (x["p"].as_str().unwrap().parse().unwrap(), round_to_precision(x["s"].as_f64().unwrap() * contract_value, *contract_value)))
                    .collect();
                let bids: Vec<(f64, f64)> = data["bids"].as_array().unwrap().iter()
                    .map(|x| (x["p"].as_str().unwrap().parse().unwrap(), round_to_precision(x["s"].as_f64().unwrap() * contract_value, *contract_value)))
                    .collect();
                let symbol = contract.replace("_", "");
                let orderbook_msg = OrderbookMsg {
                    timestamp,
                    local_timestamp,
                    exchange: GATE.to_string(),
                    symbol: symbol.clone(),
                    asks,
                    bids,
                };

                let orderbooks = symbol_orderbooks.get_mut(&contract).unwrap();
                orderbooks.push(orderbook_msg);

                if orderbooks.len() >= 100 {
                    let date = Local::now().format("%Y-%m-%d");
                    let file_path = format!("data/{GATE}/{symbol}/orderbook/{date}.csv");
                    write_orderbook_csv(&*file_path, orderbooks);
                    orderbooks.clear();
                }
            }
        }
    });

    tokio::spawn({
        let binance_symbols = binance_symbols.clone();
        async move {
            let mut symbol_trades: HashMap<String, Vec<TradeMsg>> = binance_symbols.iter()
                .map(|x| (x.to_string(), vec![]))
                .collect();

            while let Ok(msg) = binance_trade_rx.recv() {
                let local_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as i64;
                let parsed_msg: serde_json::Value = serde_json::from_str(&msg.json).unwrap();
                let data = parsed_msg["data"].as_object().unwrap();
                let symbol = data["s"].as_str().unwrap().to_string();
                let timestamp = data["T"].as_i64().unwrap();
                let trade_msg = TradeMsg {
                    timestamp,
                    local_timestamp,
                    exchange: BINANCE.to_string(),
                    symbol: symbol.clone(),
                    side: if data["m"] == true { "SELL".to_string() } else { "BUY".to_string() },
                    price: data["p"].as_str().unwrap().parse().unwrap(),
                    qty: data["q"].as_str().unwrap().parse().unwrap(),
                };

                let trades = symbol_trades.get_mut(&symbol).unwrap();
                trades.push(trade_msg);

                if trades.len() >= 100 {
                    let date = Local::now().format("%Y-%m-%d");
                    let file_path = format!("data/{BINANCE}/{symbol}/trade/{date}.csv");
                    write_trade_csv(&*file_path, trades);
                    trades.clear();
                }
            }
        }
    });

    tokio::spawn({
        let gate_symbols = Arc::clone(&gate_symbols);
        let gate_markets = Arc::clone(&gate_markets);
        async move {
            let mut symbol_trades: HashMap<String, Vec<TradeMsg>> = gate_symbols.iter()
                .map(|x| (x.to_string(), vec![]))
                .collect();

            while let Ok(msg) = gate_trade_rx.recv() {
                let local_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as i64;
                let parsed_msg: serde_json::Value = serde_json::from_str(&msg.json).unwrap();
                let data = parsed_msg["result"].as_array().unwrap();

                for item in data {
                    let timestamp = item["create_time_ms"].as_i64().unwrap();
                    let contract = item["contract"].as_str().unwrap().to_string();
                    let contract_value = gate_markets.get(&contract).unwrap().contract_value.as_ref().unwrap();
                    let qty = item["size"].as_f64().unwrap() * contract_value; // 合约张数 -> 币数
                    let symbol = contract.replace("_", "");
                    let side = if qty > 0.0 { "BUY" } else { "SELL" };
                    let trade_msg = TradeMsg {
                        timestamp,
                        local_timestamp,
                        exchange: GATE.to_string(),
                        symbol,
                        side: side.to_string(),
                        price: item["price"].as_str().unwrap().parse().unwrap(),
                        qty: round_to_precision(qty.abs(), *contract_value),
                    };
                    let trades = symbol_trades.get_mut(&contract).unwrap();
                    trades.push(trade_msg);
                }

                for (contract, trades) in symbol_trades.iter_mut() {
                    if trades.len() >= 100 {
                        let date = Local::now().format("%Y-%m-%d");
                        let symbol = contract.replace("_", "");
                        let file_path = format!("data/{GATE}/{symbol}/trade/{date}.csv");
                        write_trade_csv(&*file_path, trades);
                        trades.clear();
                    }
                }
            }
        }
    });

    tokio::signal::ctrl_c().await.unwrap();
    println!("Ctrl-C received, shutting down");
}