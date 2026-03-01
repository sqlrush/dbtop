# DBTOP 运行手册

## 一、工具简介

DBTOP 是一个基于 Python/ncurses 的 Oracle 数据库实时监控工具（类似 htop，但面向数据库）。提供 6 个实时监控面板和 8 个应急预案模块，支持终端内交互式操作。

---

## 二、部署信息

| 项目 | 值 |
|------|----|
| 部署路径 | `/home/oracle/dbtop` |
| 服务器 | `8.147.58.3` |
| 运行用户 | `oracle` |
| Python 版本 | 3.6.8 |
| Oracle 驱动 | cx_Oracle 8.3.0 |
| 数据库版本 | Oracle 19c Enterprise Edition |
| 连接方式 | SYSDBA 本地免密连接 |

---

## 三、前置条件

1. 必须以 `oracle` OS 用户运行（SYSDBA 免密认证依赖此用户身份）
2. `ORACLE_HOME` 和 `ORACLE_SID` 环境变量需正确设置（oracle 用户的 .bash_profile 中已配置）
3. 终端窗口建议宽度 ≥ 160 列，高度 ≥ 50 行
4. 告警日志文件需提前创建并授权（已配置）：
   ```bash
   # 以 root 用户执行（已完成）
   touch /var/log/dbtop_alarm.log && chown oracle:oinstall /var/log/dbtop_alarm.log
   ```

---

## 四、快速启动

```bash
# 1. 切换到 oracle 用户
su - oracle

# 2. 进入部署目录
cd /home/oracle/dbtop

# 3. 启动（使用默认配置：SYSDBA 连接，3 秒刷新）
./run_dbtop.sh

# 4. 退出：按 q 键
```

---

## 五、命令行参数

```
./run_dbtop.sh [选项]

选项：
  -u USER           数据库用户名（默认：system）
  -H HOST           数据库主机地址（默认：localhost）
  -p PORT           监听端口（默认：1521）
  -s SERVICE_NAME   Oracle 服务名（默认：orcl）
  -i INTERVAL       刷新间隔，单位秒（默认：3）
  -l LOG_INTERVAL   日志持久化间隔，为 0 不开启（默认：0）
  -d                以 daemon 模式运行（后台守护，仅运行应急预案）
```

### 常用启动示例

```bash
# 默认 SYSDBA 模式启动
./run_dbtop.sh

# 指定 1 秒刷新间隔
./run_dbtop.sh -i 1

# 以指定用户远程连接（需在 dbtop.cfg 中设 sysdba=false）
./run_dbtop.sh -u system -H 192.168.1.100 -p 1521 -s orcl

# daemon 模式（后台运行，自动触发应急预案）
./run_dbtop.sh -d

# daemon 模式 + 日志持久化，每 10 秒保存一次快照
./run_dbtop.sh -d -l 10
```

---

## 六、界面说明

启动后界面从上到下共分为 5 个区域：

### 6.1 DB Monitor（第 1 行）

| 指标 | 说明 |
|------|------|
| Oracle 版本 | 数据库版本号 |
| 用户名 | 当前连接使用的数据库用户 |
| 当前时间 | 格式 yyyy-mm-dd hh:mm:ss |
| 启动时间 | 数据库实例启动时间 |
| ROLE | 数据库角色（PRIMARY / PHYSICAL STANDBY） |
| SGA (MB) | SGA 内存总用量 |
| PGA (MB) | PGA 内存总分配量 |
| db% | 数据库繁忙度（DB CPU / DB Time） |
| WTR% | 等待时间占比 |

### 6.2 OS Monitor（第 2-3 行）

| 指标 | 说明 |
|------|------|
| LOAD | 操作系统 1 分钟负载 |
| %CPU | CPU 使用率 |
| %MEM | 内存使用率 |
| r/s / w/s | 每秒 IO 读/写次数 |
| rkB/s / wkB/s | 每秒 IO 读/写大小（kB） |
| r_await / w_await | 平均每次读/写等待时间（毫秒） |
| r_asize / w_asize (kB) | 平均每次读/写 IO 大小 |
| aqu-sz | 磁盘请求队列平均长度 |

### 6.3 Instance Monitor（第 4-5 行，滚动显示历史）

| 指标 | 说明 |
|------|------|
| SN | 当前用户会话总数 |
| AN | 非空闲等待的活跃会话数 |
| ASC | 正在执行 SQL 的活跃会话数（ON CPU） |
| ASI | 正在等待 IO 的活跃会话数 |
| IDL | 空闲会话数 |
| MBPS | 数据库进程 IO 吞吐量 |
| TPS | 每秒事务数 |
| QPS | 每秒执行数 |
| P95(ms) | SQL 执行 P95 时延 |
| REDO(kB/s) | Redo 日志生成速率 |
| CONNECTION(c/m) | 连接数使用率（当前/最大） |
| PROCESSES | 进程数 |

### 6.4 Event Monitor（中间区域）

| 列 | 说明 |
|----|------|
| EVENT | 等待事件名称 |
| TOTAL WAITS | 统计周期内等待次数 |
| TIME(us) | 等待总耗时（微秒） |
| AVG(us) | 平均单次等待耗时 |
| PCT | 占总耗时百分比 |
| WAIT_CLASS | 等待事件类别 |

按 `r` 键切换为实时模式，按 `c` 键切换为累计模式。

### 6.5 Session Monitor（底部区域）

| 列 | 说明 |
|----|------|
| SID | Oracle 会话 ID |
| USR | 用户名 |
| PROG | 应用程序名 |
| PGA(m) | 会话 PGA 内存占用（MB） |
| SQLID | 当前执行的 SQL_ID |
| SQL | SQL 语句文本（截断显示） |
| OPN | SQL 操作类型 |
| BLOCKER | 阻塞者 SID |
| E/T(ms) | SQL 执行耗时（毫秒） |
| STA | 会话状态（ACTIVE/INACTIVE） |
| STE | 细分状态（ON CPU / USR I/O / WAITING / IDLE） |
| EVENT | 当前等待事件 |
| BLK | 阻塞标识（H=持有锁 W=等待锁 H&W=两者都是） |

---

## 七、快捷键操作

### 7.1 主界面

| 按键 | 功能 |
|------|------|
| `q` | 退出工具 |
| `r` | 切换事件监控为实时模式 |
| `c` | 切换事件监控为累计模式 |
| `s` | 进入会话选择模式 |
| `m` | 进入内存监控视图 |
| `e` | 进入应急预案视图（有应急触发时） |

### 7.2 会话选择模式（按 `s` 进入）

| 按键 | 功能 |
|------|------|
| ↑ / ↓ | 上下移动选择会话 |
| ← / → | 左右滚动查看更多列 |
| `n` | 向下翻页 |
| `N` | 向上翻页 |
| `t` | 按 SQL 执行耗时排序 |
| `m` | 按 PGA 内存排序 |
| `e` | 按等待事件排序 |
| `p` | 查看选中会话详情（SQL 文本、执行计划、阻塞树等） |
| `k` | 终止选中的会话（需配置启用） |
| `K` | 终止与选中会话相同 SQL_ID 的所有会话（需配置启用） |
| 其他任意键 | 退出会话选择模式 |

### 7.3 内存监控视图（按 `m` 进入）

| 按键 | 功能 |
|------|------|
| ↑ / ↓ / ← / → | 上下左右导航 |
| `k` | 终止选中会话（需配置启用） |
| `q` 或其他键 | 退出内存视图 |

显示内容：

| 列 | 说明 |
|----|------|
| DATE | 采集时间 |
| SGA_MAX | SGA 最大值 |
| SGA_USED% | SGA 使用率 |
| SGA_FREE | SGA 空闲量 |
| SGA_FREE% | SGA 空闲比例 |
| PGA_ALLOC | PGA 总分配量 |
| PGA_USED | PGA 使用量 |
| PGA_FREE | PGA 空闲量 |
| PGA_FREE% | PGA 空闲比例 |

### 7.4 应急预案视图（按 `e` 进入）

| 按键 | 功能 |
|------|------|
| ↑ / ↓ / ← / → | 上下左右导航 |
| `k` | 执行应急命令（需配置启用） |
| 其他任意键 | 退出应急视图 |

---

## 八、应急预案模块

DBTOP 内置 8 个应急预案模块，在后台自动分析。当满足触发条件时会在界面显示告警，可按 `e` 进入查看详情。

| 模块 | 说明 | 关键配置 |
|------|------|---------|
| plan_change | 执行计划突变检测 | `os_cpu_thresh=60` |
| performance_jitter | 性能抖动检测 | `ins_acs_pct_thresh=2` |
| slow_sql | 慢 SQL 检测与处置 | `terminate=false`（默认不自动终止） |
| cpu_full | CPU 打满检测 | `os_cpu_thresh=80` |
| io_full | IO 打满检测 | `io_aqu_sz_thresh=20` |
| memory_full | 内存（SGA/PGA）满检测 | `pga_pct_thresh=80, sga_free_pct_thresh=10` |
| sessions_full | 进程/会话耗尽检测 | `sessions_full_thresh=70` |
| connections_full | 连接数耗尽检测 | `connections_full_thresh=90` |

---

## 九、配置文件说明

配置文件路径：`/home/oracle/dbtop/dbtop.cfg`

### 9.1 [main] 核心配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| interval | 3 | 刷新间隔（秒） |
| log_interval | 0 | 日志持久化间隔，0=不开启 |
| mem_interval | 30 | 内存大盘刷新间隔（秒） |
| user | "system" | 数据库用户名 |
| password_free | false | 是否免密 |
| sysdba | true | 是否使用 SYSDBA 模式 |
| host | "localhost" | 数据库主机 |
| port | 1521 | 监听端口 |
| service_name | "orcl" | Oracle 服务名 |

### 9.2 [emergency] 应急配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| enable | true | 应急预案总开关 |
| max_snapshot_number | 30 | 内存快照最大保存数 |

### 9.3 [emergency.slow_sql] 慢 SQL 配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| terminate | false | 是否自动终止慢 SQL |
| exclude_users | "SYS;SYSTEM" | 排除的用户（分号分隔） |

> 其他应急模块配置项详见 `dbtop.cfg` 文件内注释。

---

## 十、Daemon 模式

Daemon 模式下 DBTOP 在后台运行，不显示 TUI 界面，仅运行应急预案模块进行自动监控和处置。

```bash
# 启动 daemon 模式
./run_dbtop.sh -d

# 配合日志持久化
./run_dbtop.sh -d -l 10

# 后台运行（nohup）
nohup ./run_dbtop.sh -d -l 10 > /dev/null 2>&1 &
```

运行日志存放位置：
- 监控数据日志：`/home/oracle/dbtop/logs/`
- 应急预案日志：`/home/oracle/dbtop/logs_emergency/`
- 告警日志：`/var/log/dbtop_alarm.log`（需确保 oracle 用户有写权限）
- 运行日志：`/home/oracle/dbtop/dbtop_run.log`

---

## 十一、常见问题

### Q1: 启动报错 `No module named 'cx_Oracle'`
确保以 `oracle` 用户运行，环境变量 `ORACLE_HOME` 和 `LD_LIBRARY_PATH` 正确：
```bash
su - oracle
echo $ORACLE_HOME
python3 -c "import cx_Oracle; print(cx_Oracle.version)"
```

### Q2: 启动报错 `ORA-01031: insufficient privileges`
当前用户无 SYSDBA 权限。确保以 `oracle` OS 用户运行并且 `dbtop.cfg` 中 `sysdba = true`。

### Q3: 界面显示混乱
终端窗口太小，建议调整为至少 160x50。或者通过 SSH 连接时：
```bash
export TERM=xterm-256color
```

### Q4: 如何修改刷新间隔
编辑 `dbtop.cfg` 中的 `interval` 值，或启动时指定 `-i` 参数：
```bash
./run_dbtop.sh -i 1    # 1秒刷新
```

### Q5: 如何只查看不执行终止操作
默认配置下所有 `terminate` 选项均为 `false`，不会自动终止任何会话。手动按 `k` 终止会话前也会二次确认。

### Q6: 如何停止 daemon 模式
```bash
ps aux | grep dbtop | grep -v grep
kill <PID>
```

---

## 十二、文件目录结构

```
/home/oracle/dbtop/
├── dbtop.cfg                  # 主配置文件
├── run_dbtop.sh               # 启动脚本
├── requirements.txt           # Python 依赖
├── common/                    # 公共模块
│   ├── config.py              #   配置管理
│   ├── log.py                 #   日志工具
│   ├── util.py                #   数据库连接 + 查询执行
│   ├── constants.py           #   常量 + PL/SQL 模板
│   ├── alarm.py               #   告警系统
│   └── data_logger.py         #   数据持久化
├── monitor/                   # 监控模块
│   ├── monitor_base.py        #   监控基类
│   ├── db.py + db.cfg         #   数据库信息监控
│   ├── instance.py + instance.cfg  #   实例监控
│   ├── event.py + event.cfg   #   等待事件监控
│   ├── session.py + session.cfg    #   会话监控
│   ├── operating_system.py + os.cfg  #  OS 资源监控
│   └── memory.py + memory.cfg #   内存监控（SGA/PGA）
├── emergency/                 # 应急预案模块
│   ├── emergency.py           #   应急调度器
│   ├── emergency_base.py      #   应急基类
│   ├── persist.py             #   数据库持久化
│   ├── mem_persist.py         #   内存持久化
│   ├── plan_change.py         #   执行计划突变
│   ├── performance_jitter.py  #   性能抖动
│   ├── slow_sql.py            #   慢 SQL
│   ├── cpu_full.py            #   CPU 打满
│   ├── io_full.py             #   IO 打满
│   ├── memory_full.py         #   内存满
│   ├── sessions_full.py       #   进程/会话耗尽
│   └── connections_full.py    #   连接数耗尽
└── tool/
    └── dbtop.py               # 主入口
```
