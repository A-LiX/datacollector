#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/error.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <simdjson.h>
#include <spdlog/spdlog.h>                   // 引入 spdlog
#include <spdlog/sinks/basic_file_sink.h>    // 引入文件日志 sink
#include <spdlog/sinks/rotating_file_sink.h> // 引入滚动文件日志 sink
#include <spdlog/sinks/daily_file_sink.h>    // 引入按时间滚动日志 sink
#include <spdlog/async.h>                    // 引入异步日志支持

// Define SIMDJSON_PADDING if not already defined
#include <clickhouse/client.h>
#include <chrono>

#include <iostream>
#include <fstream>
#include <string>

namespace beast = boost::beast;         // Boost.Beast 命名空间
namespace websocket = beast::websocket; // WebSocket 相关
namespace net = boost::asio;            // Boost.Asio 命名空间
namespace ssl = boost::asio::ssl;       // SSL 命名空间
using tcp = net::ip::tcp;               // TCP

#define TRADE_BUFFER_SIZE 8192

int main()
{

#if defined(__linux__)
#define _GNU_SOURCE
#include <sched.h>
#include <stdlib.h>
#include <unistd.h>
    // 设置 CPU 亲和性
    cpu_set_t mask;
    CPU_ZERO(&mask);   // 清空 CPU 集合
    CPU_SET(1, &mask); // 设置 CPU 核心 1
    // 设置当前进程的 CPU 亲和性
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1)
    {
        perror("sched_setaffinity");
        exit(EXIT_FAILURE);
    }
#endif

    using namespace clickhouse;
    // 1. 创建 ClickHouse 客户端（TCP 协议）
    Client client(ClientOptions().SetHost("localhost").SetPort(9000));

    // 配置 spdlog 异步日志（队列大小为 8192）
    spdlog::init_thread_pool(8192, 1); // 日志队列大小为 8192，1 个后台线程处理日志
    // 配置滚动日志（按时间滚动：1周）
    auto weekly_logger = spdlog::create_async<spdlog::sinks::daily_file_sink_mt>(
        "weekly_logger", "./logs/depth_data_collector.log", 0, 0); // 每天午夜滚动
    spdlog::set_default_logger(weekly_logger);
    spdlog::set_level(spdlog::level::debug); // 设置日志等级为 debug
    spdlog::flush_on(spdlog::level::debug);  // 确保 debug 以上的日志立即写入文件

    spdlog::info("Starting WebSocket client...");

CONNECT:
    // 1. 创建 io_context
    net::io_context ioc;
    spdlog::info("Created io_context");

    // 2. 设置 SSL 上下文，使用 TLSv1.2 客户端
    ssl::context ctx(ssl::context::tlsv12_client);
    ctx.set_verify_mode(ssl::verify_none);
    spdlog::info("Configured SSL context");

    // 3. 定义服务器信息和目标
    const std::string host = "stream.binance.com";
    const std::string port = "9443";
    // 支持更高档位：depth100 或 depth1000
    const std::string target = "/ws/btcusdt@depth20@100ms"; // 或 "/ws/btcusdt@depth1000@100ms"
    spdlog::info("Defined server information: host={}, port={}, target={}", host, port, target);

    // 4. DNS 解析
    tcp::resolver resolver(ioc);
    boost::system::error_code err;
    auto const results = resolver.resolve(host, port, err);
    if (err)
    {
        spdlog::error("DNS resolution failed: {}", err.message());
        return EXIT_FAILURE;
    }
    spdlog::info("DNS resolution succeeded");

    // 5. 创建 Boost.Beast SSL WebSocket 流对象
    websocket::stream<beast::ssl_stream<tcp::socket>> ws(ioc, ctx);
    spdlog::info("Created WebSocket stream");

    // 7. 连接到目标 TCP 端点
    net::connect(ws.next_layer().next_layer(), results.begin(), results.end(), err);
    if (err)
    {
        spdlog::error("TCP connection failed: {}", err.message());
        return EXIT_FAILURE;
    }
    spdlog::info("TCP connection succeeded");

    // 8. 执行 SSL 握手
    ws.next_layer().handshake(ssl::stream_base::client, err);
    if (err)
    {
        spdlog::error("SSL handshake failed: {}", err.message());
        return EXIT_FAILURE;
    }
    spdlog::info("SSL handshake succeeded");

    // 9. 执行 WebSocket 握手
    ws.handshake(host, target, err);
    if (err)
    {
        spdlog::error("WebSocket handshake failed: {}", err.message());
        return EXIT_FAILURE;
    }
    spdlog::info("WebSocket handshake succeeded");
    spdlog::info("Connected to Binance WebSocket at {}", host);

    // 10. 循环读取并解析消息
    beast::flat_buffer buffer;
    // buffer.prepare(TRADE_BUFFER_SIZE); // 移除这行，让 flat_buffer 自动扩容
    simdjson::ondemand::parser parser; // 创建 simdjson 解析器
    while (true)
    {
        ws.read(buffer, err);
        if (err)
        {
            spdlog::error("WebSocket read failed: {}", err.message());
            ws.close(websocket::close_code::normal, err);
            goto CONNECT;
        }

        size_t size = buffer.size();
        // beast::buffers_to_string 返回 std::string，直接用于 padded_string
        simdjson::padded_string padded_json(beast::buffers_to_string(buffer.data()));

        auto doc_result = parser.iterate(padded_json);
        if (doc_result.error())
        {
            spdlog::error("JSON Parsing Error: {} | Raw: {}", simdjson::error_message(doc_result.error()), std::string(padded_json));
            buffer.consume(size);
            continue;
        }

        auto &doc = doc_result.value_unsafe();
        if(doc.is_null())
        {
            spdlog::error("Received null JSON document");
            buffer.consume(size);
            continue;
        }

        // depth 数据结构:
        // {
        //   "lastUpdateId": 70260382910,
        //   "bids": [["104854.02000000","1.77263000"], ...],
        //   "asks": [["104854.03000000","8.04331000"], ...]
        // }

        uint64_t last_update_id = doc["lastUpdateId"].get_uint64().value_unsafe();

        uint64_t local_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count();

        // 解析 bids 和 asks
        auto bids_arr = doc["bids"].get_array().value_unsafe();
        auto asks_arr = doc["asks"].get_array().value_unsafe();

        // ClickHouse 列
        auto col_last_update_id = std::make_shared<clickhouse::ColumnUInt64>();
        auto col_side = std::make_shared<clickhouse::ColumnString>();
        auto col_price = std::make_shared<clickhouse::ColumnFloat64>();
        auto col_quantity = std::make_shared<clickhouse::ColumnFloat64>();
        auto col_local_ts = std::make_shared<clickhouse::ColumnUInt64>();

        // 写入 bids
        for (auto bid : bids_arr) {
            auto bid_val = bid.get_array().value_unsafe();
            std::cout << "Bid: " << bid_val << std::endl;
            auto bid_price_sv = bid_val.at(0).get_string().value_unsafe();
            std::cout<< "Bid Price: " << bid_price_sv << std::endl;
            auto bid_qty_sv = bid_val.at(1).get_string().value_unsafe();
            double bid_price = std::stod(std::string(bid_price_sv));
            double bid_qty = std::stod(std::string(bid_qty_sv));
            col_last_update_id->Append(last_update_id);
            col_side->Append("bid");
            col_price->Append(bid_price);
            col_quantity->Append(bid_qty);
            col_local_ts->Append(local_time_ns);
        }
        // 写入 asks
        for (auto ask : asks_arr) {
            auto ask_val = ask.get_array().value_unsafe();
            auto ask_price_sv = ask_val.at(0).get_string().value_unsafe();
            auto ask_qty_sv = ask_val.at(1).get_string().value_unsafe();
            double ask_price = std::stod(std::string(ask_price_sv));
            double ask_qty = std::stod(std::string(ask_qty_sv));
            col_last_update_id->Append(last_update_id);
            col_side->Append("ask");
            col_price->Append(ask_price);
            col_quantity->Append(ask_qty);
            col_local_ts->Append(local_time_ns);
        }

        // 构造 Block 并添加列
        clickhouse::Block block;
        block.AppendColumn("last_update_id", col_last_update_id);
        block.AppendColumn("side", col_side);
        block.AppendColumn("price", col_price);
        block.AppendColumn("quantity", col_quantity);
        block.AppendColumn("local_timestamp_ns", col_local_ts);

        // 插入到 ClickHouse
        client.Insert("default.depth_data", block);

        buffer.consume(size);
    }

    // 11. 关闭连接
    ws.close(websocket::close_code::normal, err);
    if (err)
    {
        spdlog::error("WebSocket close failed: {}", err.message());
        return EXIT_FAILURE;
    }
    spdlog::info("WebSocket connection closed successfully");

    return EXIT_SUCCESS;
}

//  g++ -std=c++17 -O2 -o depth_data_collector \
    /Users/lixiang/cppTest/matching_engine/depth_data_collector.cpp \
    -I/opt/homebrew/include \
    -L/opt/homebrew/lib \
    -lboost_system -lssl -lcrypto -lpthread -lsimdjson \
    -lfmt -lclickhouse-cpp-lib  -lcityhash -llz4 -labsl_base