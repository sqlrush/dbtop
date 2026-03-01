# dbtop 项目

## 项目概述
dbtop — 一个数据库实时监控工具（类似 htop，但面向数据库）。

## 工作目录
/Users/yingjiewang/sqlparser/dbtop

## 技术栈
- Python 3.6+
- cx_Oracle / oracledb
- curses (终端 UI)

## 项目结构
- `tool/dbtop.py` — 主程序入口，包含 `main()` 函数
- `tool/dbtop.cfg` — 配置文件（pip 安装时打包）
- `tool/benchmark.py` — TPS/QPS 压测工具
- `monitor/` — 监控模块
- `setup.py` — pip 安装配置（entry_points: dbtop 命令）
- `run_dbtop.sh` — 启动脚本

## 服务器信息
- 地址: root@8.147.58.3, 数据库用户: oracle
- 16 核 CPU, 61G 内存, Oracle 19c

## 开发约定
- 默认 SYSDBA 免密登录
- 非 SYSDBA 模式密码配置在 dbtop.cfg 中
- 支持 `pip3 install . --user` 安装后 `dbtop` 命令直接运行

## 用户提问记录

### Oracle 等待事件与性能指标
1. **Event 的 PCT 是如何统计的？**
   - PCT = event_wait_time / (sum_all_non_idle_waits + DB_CPU) × 100
   - 分母不含空闲等待事件

2. **右上角 db% 是如何计算的？**
   - db% = DB_CPU_delta / (time_interval × nproc) × 100
   - 表示数据库 CPU 利用率占总 CPU 容量的百分比

3. **为什么 event 里 DB CPU 的时间占比比 db% 少很多？**
   - db% 是 CPU 占总容量的比例，event PCT 是 CPU 占 DB time 的比例
   - 当存在大量非空闲等待事件时，DB CPU 在 event PCT 中的占比会被稀释

4. **为什么排名第一的等待事件是 resmgr:cpu quantum？**
   - Oracle Resource Manager CPU 限流，进程被挂起等待 CPU 时间片
   - 解决：`ALTER SYSTEM SET resource_manager_plan = '' SCOPE=MEMORY`

5. **enq: HW - contention 等待事件很高，这是什么原因？**
   - High Water Mark 高水位线争用
   - INSERT 新行需要推进 HWM 分配新块，HW enqueue 是串行的
   - 大量并发 INSERT 时成为瓶颈
   - 优化：增加分区数、降低 INSERT 比例、预分配空间

6. **buffer busy waits 原因及优化？**
   - 多个 session 争用同一个数据块
   - 原因：表太小（10K 行 ≈ 200 块），200 个 worker 集中在少数块上
   - 优化：扩展到 1M 行 + HASH 32 分区（8192 块），分散热点

### Oracle 时间模型
7. **空闲等待事件的时间是不是和 DB time 完全不重合？DB time 仅包括 CPU 和非空闲等待事件时间？**
   - 是的。DB time = DB CPU + 非空闲等待时间
   - 空闲等待（如 SQL*Net message from client）不计入 DB time

8. **GaussDB 的等待事件、CPU time 和 DB time 是否也是这样？**
   - GaussDB (openGauss) 基于 PostgreSQL，时间模型不同
   - 没有 Oracle 式的 DB time 概念，使用 pg_stat_activity 的 wait_event

9. **MySQL 呢，和 Oracle 一样吗？**
   - MySQL Performance Schema 有等待事件体系
   - 没有 Oracle 的 DB time / DB CPU 概念
   - 通过 events_waits_summary 系列表统计

### 锁与并发控制
10. **读写锁和互斥锁有什么区别？**
    - 互斥锁 (Mutex)：同一时刻只允许一个线程访问（读写都互斥）
    - 读写锁 (RWLock)：允许多个读者并发，写者独占

11. **Oracle 和 MySQL 里互斥锁和读写锁分别用来保护什么资源？**
    - Oracle: Mutex 保护 cursor/SQL 执行计划缓存 (library cache)；Latch 保护 buffer cache、redo allocation 等 SGA 内存结构
    - MySQL: Mutex 保护 InnoDB 内部状态；RWLock 保护 buffer pool、AHI 等

12. **等待 mutex 锁的时候释放 CPU 吗？**
    - 两阶段：spin（自旋消耗 CPU）→ sleep（释放 CPU，进入等待事件）
    - cursor: pin S 等待事件只统计 sleep 阶段，spin 阶段计入 DB CPU

13. **Oracle 里互斥锁就是 mutex，读写锁就是 latch，是吗？**
    - 不完全对。Latch 本质也是互斥锁（low-level spin lock）
    - Oracle 的分类：Mutex（轻量，保护 cursor pin 等）、Latch（保护 SGA 结构）、Enqueue（行锁等队列锁）
    - Oracle 没有显式的 RWLock，latch 在某些场景支持 shared 模式

14. **所以等 mutex 部分时间是在 CPU time，部分时间在 cursor: pin S？**
    - 是的。spin 阶段 → DB CPU；sleep 阶段 → cursor: pin S wait event

15. **为什么需要互斥锁，连读和读之间也要互斥？是因为读会引发后续的写操作吗？**
    - cursor: pin S 的 S = Shared，是共享模式，读与读不互斥
    - 争用发生在：共享读 vs 排他写（如 hard parse、age out cursor）
    - 高并发下大量 shared 请求也会因 mutex 内部 CAS 操作产生争用

### TPS/QPS 压测
16. **为什么 dbtop 上看到的 TPS 只有 3 万，benchmark 显示 10 万？**
    - dbtop 统计 v$sysstat 的 user commits
    - PL/SQL 内 COMMIT WRITE BATCH NOWAIT 被内核合并，user commits 不逐次递增
    - 空事务（DML 影响 0 行）COMMIT 不增加 user commits
