#!/bin/bash

# ==============================================================================
# 数据库控制脚本 (DB Control Script)
#
# 功能: 启动、停止、重启和检查数据库状态
# 用法: ./db_control.sh {start|stop|restart|status}
# ==============================================================================

# --- 配置变量 ---

# 数据库可执行文件的路径.
# 在开发环境，我们使用 'cargo run'.
# 在生产环境，这应该是 'target/release/goatkv_server'.
DB_EXECUTABLE="cargo run --bin goatkv_server"

# 数据库启动时需要的参数 (当前不需要).
DB_ARGS=""

# 创建一个临时目录来存放PID和日志文件
mkdir -p .tmp

# 进程ID文件 (PID File) 的路径.
PID_FILE=".tmp/goatkv_server.pid"

# 日志文件路径.
LOG_FILE=".tmp/goatkv_server.log"

# 优雅停库时，等待进程自行退出的最长时间 (秒)
SHUTDOWN_TIMEOUT=30

# --- 脚本核心逻辑 ---

# 检查数据库是否正在运行
# 如果 PID 文件存在并且对应的进程也存在，则认为在运行
is_running() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        # kill -0 $PID 不会发送信号，但可以检查进程是否存在
        if kill -0 "$PID" >/dev/null 2>&1; then
            return 0 # 0 表示 true (正在运行)
        fi
    fi
    return 1 # 1 表示 false (未运行)
}

# 启动数据库
start() {
    if is_running; then
        echo "数据库已经在运行 (PID: $(cat "$PID_FILE"))."
        exit 1
    fi

    echo "正在启动数据库..."
    # 使用 nohup 和 & 在后台启动数据库，并将日志输出到文件
    # 启动后，获取进程ID并写入PID文件
    nohup "$DB_EXECUTABLE" $DB_ARGS >> "$LOG_FILE" 2>&1 &

    # $! 是上一个后台命令的PID
    echo $! > "$PID_FILE"

    # 等待一小会儿，确认启动是否成功
    sleep 2
    if is_running; then
        echo "数据库启动成功 (PID: $(cat "$PID_FILE"))."
    else
        echo "数据库启动失败. 请检查日志文件: $LOG_FILE"
        rm -f "$PID_FILE"
        exit 1
    fi
}

# 停止数据库
stop() {
    if ! is_running; then
        echo "数据库未在运行."
        return
    fi

    PID=$(cat "$PID_FILE")
    echo "正在尝试优雅地停止数据库 (PID: $PID)..."

    # --- 优雅停库的核心 ---
    # 1. 优先使用数据库自带的关闭命令 (如果存在)
    #    例如: mysqladmin -u root -p shutdown
    #    例如: pg_ctl -D /path/to/data stop
    #    如果您的数据库有这样的命令，请在这里替换 `kill` 命令

    # 2. 如果没有自带命令，则发送 SIGTERM 信号 (kill -15)
    #    这个信号可以被进程捕获，从而执行清理操作
    kill -15 "$PID"

    # 3. 等待并验证进程是否已退出
    echo "等待数据库进程关闭 (最长 ${SHUTDOWN_TIMEOUT} 秒)..."
    counter=0
    while is_running; do
        if [ $counter -ge $SHUTDOWN_TIMEOUT ]; then
            echo "优雅停库超时!"
            break
        fi
        sleep 1
        ((counter++))
        echo -n "."
    done
    echo "" # 换行

    # 4. 如果进程仍然存在，则强制关闭
    if is_running; then
        echo "数据库未能正常关闭，将执行强制关闭 (kill -9)..."
        kill -9 "$PID"
        sleep 1
        if is_running; then
            echo "错误：无法停止数据库进程 $PID."
            exit 1
        fi
    fi

    # 5. 清理PID文件
    rm -f "$PID_FILE"
    echo "数据库已成功停止."
}

# 显示数据库状态
status() {
    if is_running; then
        echo "数据库正在运行 (PID: $(cat "$PID_FILE"))."
    else
        echo "数据库已停止."
        # 检查PID文件是否残留
        if [ -f "$PID_FILE" ]; then
            echo "警告: 发现残留的 PID 文件 ($PID_FILE)，但进程不存在. 已为您清理."
            rm -f "$PID_FILE"
        fi
    fi
}

# --- 命令分发 ---
case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    status)
        status
        ;;
    restart)
        echo "正在重启数据库..."
        stop
        sleep 2
        start
        ;;
    *)
        echo "用法: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac

exit 0
