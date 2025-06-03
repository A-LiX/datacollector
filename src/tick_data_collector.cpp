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

#define TRADE_BUFFER_SIZE 256

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
        "weekly_logger", "./logs/tick_data_collector.log", 0, 0); // 每天午夜滚动
    spdlog::set_default_logger(weekly_logger);
    spdlog::set_level(spdlog::level::debug); // 设置日志等级为 debug
    spdlog::flush_on(spdlog::level::debug);  // 确保 debug 以上的日志立即写入文件

    spdlog::info("Starting WebSocket client...");

    // 1. 创建 io_context
CONNECT:
    net::io_context ioc;
    spdlog::info("Created io_context");

    // 2. 设置 SSL 上下文，使用 TLSv1.2 客户端
    ssl::context ctx(ssl::context::tlsv12_client);
    ctx.set_verify_mode(ssl::verify_none);
    spdlog::info("Configured SSL context");

    // 3. 定义服务器信息和目标
    const std::string host = "stream.binance.com";
    const std::string port = "9443";
    // const std::string target = "/ws/btcusdt@depth@100ms"; // 目标路径
    const std::string target = "/ws/btcusdt@trade"; // 目标路径
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

    // 设置 WebSocket 读取超时为 5 秒
    ws.set_option(websocket::stream_base::timeout{
        std::chrono::seconds(5), // handshake timeout
        std::chrono::seconds(5), // idle timeout
        false                    // no ping
    });

    // 10. 循环读取并解析消息
    beast::flat_buffer buffer;
    // websocket::frame_type fin; // 移除这一行
    buffer.prepare(TRADE_BUFFER_SIZE); // 使用宏定义的 buffer 大小
    simdjson::ondemand::parser parser; // 创建 simdjson 解析器
    while (true)
    {
        // 读取消息
        //ws.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
        ws.read(buffer, err); // 只传 buffer 和 err
        if (err)
        {
            spdlog::error("WebSocket read failed: {}", err.message());
            ws.close(websocket::close_code::normal, err);
            spdlog::info("Reconnecting...");
            goto CONNECT;
        }

        // 判断是否收到 ping 帧（通过 WebSocket 控制帧回调处理）
        // Binance 不会发 ping 数据帧到业务流，ping/pong 通常由底层自动处理
        // 如果你想检测文本消息内容为 "ping"，可这样判断：
        size_t size = buffer.size();
        auto data = static_cast<const char *>(buffer.data().data());
        std::string_view msg(data, size);
        

        // 解析 JSON 消息
        auto doc_result = parser.iterate(data, size, TRADE_BUFFER_SIZE);
        if (doc_result.error())
        {
            spdlog::error("JSON Parsing Error: {}", simdjson::error_message(doc_result.error()));
            buffer.consume(size);
            continue;
        }

        // std::cout << "Received message: " << doc_result.value_unsafe() << std::endl;
        auto &doc = doc_result.value_unsafe();

        // 提取字段值
        //{"e":"trade","E":1748688945753,"s":"BTCUSDT","t":4973345474,"p":"103394.30000000","q":"0.00053000","T":1748688945753,"m":false,"M":true}
        std::string_view event_type = doc["e"].get_string().value_unsafe();
        uint64_t event_time_ms = doc["E"].get_uint64().value_unsafe();
        std::string_view symbol = doc["s"].get_string().value_unsafe();
        uint64_t trade_id = doc["t"].get_uint64().value_unsafe();
        // 先读取字符串，再转为 double
        std::string_view quantity_str = doc["q"].get_string().value_unsafe();
        std::string_view price_str = doc["p"].get_string().value_unsafe();
        double quantity = std::stod(std::string(quantity_str));
        double price = std::stod(std::string(price_str));
        uint64_t trade_time_ms = doc["T"].get_uint64().value_unsafe();
        bool is_buyer_maker = doc["m"].get_bool().value_unsafe();
        bool is_market_match = doc["M"].get_bool().value_unsafe();
        // double price = std::stod(std::string(doc["p"].get_string().value_unsafe()));

        uint64_t local_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count();

        auto col_event_type = std::make_shared<ColumnString>();
        auto col_event_time = std::make_shared<ColumnDateTime64>(3); // 毫秒
        auto col_symbol = std::make_shared<ColumnString>();
        auto col_trade_id = std::make_shared<ColumnUInt64>();
        auto col_price = std::make_shared<ColumnFloat64>();
        auto col_quantity = std::make_shared<ColumnFloat64>();
        auto col_trade_time = std::make_shared<ColumnDateTime64>(3);
        auto col_is_buyer_maker = std::make_shared<ColumnUInt8>();
        auto col_is_market_match = std::make_shared<ColumnUInt8>();
        auto col_local_ts = std::make_shared<ColumnUInt64>();

        // 添加数据（1 行）
        col_event_type->Append(event_type);
        col_event_time->Append(event_time_ms);
        col_symbol->Append(symbol);
        col_trade_id->Append(trade_id);
        col_price->Append(price);
        col_quantity->Append(quantity);
        col_trade_time->Append(trade_time_ms);
        col_is_buyer_maker->Append(is_buyer_maker);
        col_is_market_match->Append(is_market_match);
        col_local_ts->Append(local_time_ns);

        // 4. 构造 Block 并添加列
        Block block;
        block.AppendColumn("event_type", col_event_type);
        block.AppendColumn("event_time", col_event_time);
        block.AppendColumn("symbol", col_symbol);
        block.AppendColumn("trade_id", col_trade_id);
        block.AppendColumn("price", col_price);
        block.AppendColumn("quantity", col_quantity);
        block.AppendColumn("trade_time", col_trade_time);
        block.AppendColumn("is_buyer_maker", col_is_buyer_maker);
        block.AppendColumn("is_market_match", col_is_market_match);
        block.AppendColumn("local_timestamp_ns", col_local_ts);

        // std::cout << "Rows in block: " << block.GetRowCount() << std::endl;

        // 5. 插入到 ClickHouse
        client.Insert("default.tick_data", block);
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

// g++ -std=c++17 -O2 -o datacollector \
    /Users/lixiang/cppTest/matching_engine/datacollector.cpp \
    -I/opt/homebrew/include \
    -L/opt/homebrew/lib \
    -lboost_system -lssl -lcrypto -lpthread -lsimdjson \
    -lfmt -lclickhouse-cpp-lib  -lcityhash -llz4 -labsl_base