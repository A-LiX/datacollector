CXX = g++
CXXFLAGS = -std=c++17 -O2 -I/opt/homebrew/include
LDFLAGS = -L/opt/homebrew/lib -lboost_system -lssl -lcrypto -lpthread -lsimdjson -lfmt -lclickhouse-cpp-lib -lcityhash -llz4 -labsl_base

# 源文件与目标文件
DEPTH_SRC = src/depth_data_collector.cpp
DEPTH_BIN = depth_data_collector

TICK_SRC = src/tick_data_collector.cpp
TICK_BIN = tick_data_collector

.PHONY: all clean

all: $(DEPTH_BIN) $(TICK_BIN)

$(DEPTH_BIN): $(DEPTH_SRC)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

$(TICK_BIN): $(TICK_SRC)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(DEPTH_BIN) $(TICK_BIN)
