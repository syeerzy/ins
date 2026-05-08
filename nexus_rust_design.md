# Nexus Rust 重写 - 架构设计文档

## 1. 设计理念

### 1.1 核心原则

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Rust 特性优势                                 │
├─────────────────┬─────────────────────────────────────────────────────┤
│ 所有权系统       │ 编译期消除数据竞争，零成本并发安全                    │
│ 生命周期         │ 借用检查器确保引用有效性，无悬垂指针                  │
│ Async/Await     │ 异步运行时(tokio)，高性能I/O复用                      │
│ Result模式       │ 类型安全的错误处理，无异常泄漏                         │
│ Trait系统        │ 依赖注入+接口抽象，模块可测试性                       │
│ Zero-Copy       │ Bytes/ByteSlice，序列化零拷贝                         │
│ 编译期安全       │ 穷尽匹配(Exhaustive Matching)，无未处理分支            │
└─────────────────┴─────────────────────────────────────────────────────┘
```

### 1.2 架构目标

| 目标 | 描述 |
|------|------|
| **类型安全** | 充分利用 Rust 类型系统，消除运行时错误 |
| **并发安全** | 编译期保证数据竞争安全，无需运行时锁检查 |
| **可测试性** | 每个模块可通过 trait 抽象进行 Mock |
| **可观测性** | 内置 metrics、tracing、structured logging |
| **性能极致** | 异步I/O、无锁数据结构、批量处理 |

---

## 2. 项目结构

```
nexus/
├── Cargo.toml
├── src/
│   ├── lib.rs                 # 库入口
│   │
│   ├── bin/                   # 可执行文件
│   │   ├── server.rs          # 服务器入口
│   │   └── client.rs          # 客户端CLI
│   │
│   ├── raft/                  # [核心] Raft 共识协议
│   │   ├── mod.rs
│   │   ├── state.rs           # Raft 状态机
│   │   ├── role.rs            # 角色状态机 (Leader/Candidate/Follower)
│   │   ├── log_manager.rs     # 日志管理
│   │   ├── vote.rs            # 投票逻辑
│   │   ├── append.rs          # 日志复制
│   │   ├── snapshot.rs        # 快照与压缩
│   │   ├── membership.rs      # 集群成员变更
│   │   └── storage.rs         # Raft 状态持久化
│   │
│   ├── protocol/              # [核心] 通信协议
│   │   ├── mod.rs
│   │   ├── message.rs         # 消息定义
│   │   ├── codec.rs           # 编解码 (MessagePack/Protobuf)
│   │   └── router.rs          # 消息路由
│   │
│   ├── transport/             # [核心] 网络传输
│   │   ├── mod.rs
│   │   ├── tcp.rs             # TCP 连接管理
│   │   ├── pool.rs            # 连接池
│   │   ├── endpoint.rs        # 端点抽象
│   │   └── framing.rs         # 帧处理
│   │
│   ├── state/                # [核心] 状态机
│   │   ├── mod.rs
│   │   ├── kv_engine.rs       # KV 存储引擎
│   │   ├── engine_trait.rs    # 引擎抽象
│   │   ├── transaction.rs     # 事务处理
│   │   └── wal.rs            # Write-Ahead Log
│   │
│   ├── storage/               # [核心] 存储层
│   │   ├── mod.rs
│   │   ├── rocksdb.rs         # RocksDB 实现
│   │   ├── memory.rs          # 内存实现 (测试用)
│   │   └── lsm.rs             # 自实现 LSM Tree
│   │
│   ├── features/             # [业务] 高级功能
│   │   ├── mod.rs
│   │   ├── lock/
│   │   │   ├── mod.rs
│   │   │   ├── manager.rs     # 锁管理器
│   │   │   └── types.rs       # 锁相关类型
│   │   ├── watch/
│   │   │   ├── mod.rs
│   │   │   ├── subscriber.rs  # 订阅管理
│   │   │   └── notifier.rs    # 事件通知
│   │   └── session/
│   │       ├── mod.rs
│   │       ├── manager.rs     # 会话管理
│   │       └── heartbeat.rs   # 心跳检测
│   │
│   ├── auth/                 # [业务] 认证授权
│   │   ├── mod.rs
│   │   ├── user.rs           # 用户管理
│   │   ├── token.rs          # Token管理
│   │   └── rbac.rs           # 权限控制
│   │
│   ├── api/                  # [接口] 客户端SDK
│   │   ├── mod.rs
│   │   ├── client.rs         # 客户端
│   │   ├── builder.rs        # Builder模式
│   │   ├── error.rs          # 错误类型
│   │   └── types.rs          # API类型定义
│   │
│   ├── server/              # [服务] 服务端
│   │   ├── mod.rs
│   │   ├── node.rs           # 节点实现
│   │   ├── handler.rs        # 请求处理
│   │   └── coordinator.rs    # 协调器
│   │
│   ├── util/                # [工具] 公共组件
│   │   ├── mod.rs
│   │   ├── config.rs         # 配置解析
│   │   ├── logging.rs        # 日志
│   │   ├── metrics.rs        # 指标
│   │   ├── tracing.rs        # 链路追踪
│   │   ├── timing.rs         # 定时器
│   │   └── buffer.rs         # 缓冲区
│   │
│   └── tests/               # 集成测试
│       ├── mod.rs
│       ├── cluster.rs        # 集群测试
│       ├── raft_tests.rs     # Raft协议测试
│       └── api_tests.rs      # API测试
│
├── proto/                   # Protobuf 定义
│   └── nexus.proto
│
├── benches/                 # 性能基准测试
│   └── throughput.rs
│
└── examples/                # 示例代码
    └── quickstart.rs
```

---

## 3. 模块详细设计

### 3.1 Raft 核心模块 `raft/`

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              Raft State Machine                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                     │
│  │   Follower  │◄──►│  Candidate  │◄──►│   Leader    │                     │
│  └─────────────┘    └─────────────┘    └─────────────┘                     │
│         │                  │                  │                              │
│         └──────────────────┼──────────────────┘                              │
│                            ▼                                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │                       RaftCore                                       │      │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐  │      │
│  │  │ current_term │ │ voted_for    │ │ commit_index│ │ last_applied│  │      │
│  │  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘  │      │
│  │                                                                       │      │
│  │  ┌─────────────────────────────────────────────────────────────┐     │      │
│  │  │                      LogManager                              │     │      │
│  │  │  [Entry0][Entry1][Entry2][Entry3][Entry4]...                │     │      │
│  │  │   term   term   term   term   term                          │     │      │
│  │  └─────────────────────────────────────────────────────────────┘     │      │
│  │                                                                       │      │
│  │  ┌─────────────────────────────────────────────────────────────┐     │      │
│  │  │                    RoleHandler                               │     │      │
│  │  │  - handle_timeout()                                          │     │      │
│  │  │  - handle_append_entries()                                    │     │      │
│  │  │  - handle_vote_request()                                     │     │      │
│  │  └─────────────────────────────────────────────────────────────┘     │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │                      RaftStorage                                     │      │
│  │  - PersistentState: current_term, voted_for, log entries            │      │
│  │  - Snapshot: last_included_index, last_included_term               │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

#### 核心类型定义

```rust
// src/raft/mod.rs

pub mod state;
pub mod role;
pub mod log_manager;
pub mod storage;
pub mod membership;
pub mod snapshot;
pub mod vote;
pub mod append;

use ::state::RaftState;
use crate::protocol::message::Message;
use std::sync::Arc;

/// Raft 节点核心
pub struct RaftCore<S: Storage> {
    /// 节点ID
    node_id: NodeId,

    /// Raft 状态
    state: RaftState,

    /// 日志管理器
    log_manager: LogManager,

    /// 角色处理器
    role: Role,

    /// 存储后端
    storage: Arc<S>,

    /// 配置
    config: RaftConfig,

    /// 发送通道
    message_tx: mpsc::Sender<Message>,

    /// 内部状态锁 (仅用于需要独占访问的场景)
    /// 大部分操作使用消息传递而非锁
    inner: RwLock<RaftCoreInner>,
}

impl<S: Storage> RaftCore<S> {
    /// 处理接收到的消息
    pub async fn step(&self, msg: Message) -> Result<()> {
        match msg.get_type() {
            MessageType::Propose => self.handle_propose(msg).await,
            MessageType::AppendEntries => self.handle_append_entries(msg).await,
            MessageType::AppendEntriesResponse => self.handle_append_response(msg).await,
            MessageType::VoteRequest => self.handle_vote_request(msg).await,
            MessageType::VoteResponse => self.handle_vote_response(msg).await,
            MessageType::Heartbeat => self.handle_heartbeat(msg).await,
            MessageType::HeartbeatResponse => self.handle_heartbeat_response(msg).await,
            MessageType::Snapshot => self.handle_snapshot(msg).await,
            MessageType::Timeout => self.handle_timeout(msg).await,
        }
    }
}

/// Raft 状态
#[derive(Debug, Clone)]
pub struct RaftState {
    /// 当前任期
    pub current_term: Term,

    /// 投票给的节点 (None 表示未投票)
    pub voted_for: Option<NodeId>,

    /// 已提交的日志索引
    pub commit_index: LogIndex,

    /// 已应用的日志索引
    pub last_applied: LogIndex,

    /// 节点角色
    pub role: RoleType,
}

/// 角色类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleType {
    Follower,
    Candidate,
    Leader,
}

/// 日志条目
#[derive(Debug, Clone)]
pub struct Entry {
    pub index: LogIndex,
    pub term: Term,
    pub data: EntryData,
}

impl Entry {
    pub fn new(index: LogIndex, term: Term, data: EntryData) -> Self {
        Self { index, term, data }
    }
}

/// 入口数据
#[derive(Debug, Clone)]
pub enum EntryData {
    /// 空白条目 (用于提交检测)
    Blank,
    /// 配置变更
    Config(ConfigChange),
    /// 应用数据
    Data { key: Vec<u8>, value: Vec<u8> },
    /// 删除操作
    Delete { key: Vec<u8> },
    /// 锁操作
    Lock { key: Vec<u8>, session_id: Uuid },
    /// 解锁操作
    Unlock { key: Vec<u8>, session_id: Uuid },
}
```

#### 角色状态机

```rust
// src/raft/role.rs

use super::*;

/// 角色处理器 trait
pub trait RoleHandler: Send + Sync {
    fn role_type(&self) -> RoleType;

    fn on_election_timeout(&self, core: &mut RaftCore<impl Storage>) -> Result<()>;

    fn on_heartbeat_timeout(&self, core: &mut RaftCore<impl Storage>) -> Result<()>;

    fn on_vote_request(&self, core: &mut RaftCore<impl Storage>, req: &VoteRequest) -> Result<VoteResponse>;

    fn on_append_entries(&self, core: &mut RaftCore<impl Storage>, req: &AppendEntriesRequest) -> Result<AppendEntriesResponse>;
}

/// Follower 处理器
pub struct FollowerHandler {
    /// 选举超时时间
    election_timeout: Duration,
    /// 上次收到消息时间
    last_activity: Instant,
}

impl FollowerHandler {
    pub fn new(timeout: Duration) -> Self {
        Self {
            election_timeout: timeout,
            last_activity: Instant::now(),
        }
    }

    pub fn update_activity(&mut self) {
        self.last_activity = Instant::now();
    }

    pub fn is_election_timeout(&self) -> bool {
        self.last_activity.elapsed() > self.election_timeout
    }
}

impl RoleHandler for FollowerHandler {
    fn role_type(&self) -> RoleType {
        RoleType::Follower
    }

    fn on_election_timeout(&self, core: &mut RaftCore<impl Storage>) -> Result<()> {
        // 转换为 Candidate，开始选举
        core.become_candidate()?;
        core.start_election()
    }

    fn on_heartbeat_timeout(&self, _core: &mut RaftCore<impl Storage>) -> Result<()> {
        // Follower 不发送心跳
        Ok(())
    }

    fn on_vote_request(&self, core: &mut RaftCore<impl Storage>, req: &VoteRequest) -> Result<VoteResponse> {
        let state = core.state();

        // 任期过期
        if req.term < state.current_term {
            return Ok(VoteResponse::new(state.current_term, false));
        }

        // 检查日志是否比请求者的更新
        let last_log = core.log_manager().last_log_entry()?;
        let log_ok = req.last_log_term > last_log.term
            || (req.last_log_term == last_log.term && req.last_log_index >= last_log.index);

        // 检查是否已经投票
        let vote_ok = state.voted_for.is_none()
            || state.voted_for == Some(req.candidate_id);

        if log_ok && vote_ok {
            core.set_voted_for(Some(req.candidate_id))?;
            Ok(VoteResponse::new(state.current_term, true))
        } else {
            Ok(VoteResponse::new(state.current_term, false))
        }
    }

    fn on_append_entries(&self, core: &mut RaftCore<impl Storage>, req: &AppendEntriesRequest) -> Result<AppendEntriesResponse> {
        // 更新活动状态
        self.update_activity();

        let state = core.state();

        // 任期过期
        if req.term < state.current_term {
            return Ok(AppendEntriesResponse::new(state.current_term, false));
        }

        // 发现新任期，转换为 Follower
        if req.term > state.current_term {
            core.set_current_term(req.term)?;
            core.become_follower(req.leader_id);
        }

        // 检查日志一致性
        let log_ok = if req.prev_log_index == 0 {
            true
        } else if req.prev_log_index < core.log_manager().first_index()? {
            // 快照之前的数据
            return Ok(AppendEntriesResponse::new(state.current_term, false, Some(ErrIndex::Snapshot)));
        } else {
            match core.log_manager().entry(req.prev_log_index)? {
                Some(entry) => entry.term == req.prev_log_term,
                None => false,
            }
        };

        if !log_ok {
            return Ok(AppendEntriesResponse::new(state.current_term, false, Some(ErrIndex::Conflict)));
        }

        // 追加新条目
        core.log_manager().append_entries(&req.entries)?;

        // 更新提交索引
        if req.leader_commit > state.commit_index {
            let commit_to = std::cmp::min(req.leader_commit, core.log_manager().last_index()?);
            core.set_commit_index(commit_to)?;
        }

        Ok(AppendEntriesResponse::new(state.current_term, true))
    }
}

/// Leader 处理器
pub struct LeaderHandler {
    /// 每个 Follower 的 next_index
    next_indices: HashMap<NodeId, LogIndex>,
    /// 每个 Follower 的 match_index
    match_indices: HashMap<NodeId, LogIndex>,
    /// 心跳间隔
    heartbeat_interval: Duration,
    /// 下次心跳时间
    next_heartbeat: Instant,
}

impl LeaderHandler {
    pub fn new(members: &[NodeId], heartbeat_interval: Duration) -> Self {
        let mut next_indices = HashMap::new();
        let mut match_indices = HashMap::new();

        for &node_id in members {
            // 初始化为 last_log_index + 1
            next_indices.insert(node_id, 0);
            match_indices.insert(node_id, 0);
        }

        Self {
            next_indices,
            match_indices,
            heartbeat_interval,
            next_heartbeat: Instant::now(),
        }
    }

    pub fn next_index(&self, node_id: NodeId) -> Option<LogIndex> {
        self.next_indices.get(&node_id).copied()
    }

    pub fn set_next_index(&mut self, node_id: NodeId, index: LogIndex) {
        self.next_indices.insert(node_id, index);
    }

    pub fn match_index(&self, node_id: NodeId) -> Option<LogIndex> {
        self.match_indices.get(&node_id).copied()
    }

    pub fn set_match_index(&mut self, node_id: NodeId, index: LogIndex) {
        self.match_indices.insert(node_id, index);
    }
}

impl RoleHandler for LeaderHandler {
    fn role_type(&self) -> RoleType {
        RoleType::Leader
    }

    fn on_election_timeout(&self, _core: &mut RaftCore<impl Storage>) -> Result<()> {
        // Leader 不会因为选举超时而改变
        Ok(())
    }

    fn on_heartbeat_timeout(&self, core: &mut RaftCore<impl Storage>) -> Result<()> {
        // 发送心跳
        self.send_heartbeats(core)
    }

    fn on_vote_request(&self, core: &mut RaftCore<impl Storage>, req: &VoteRequest) -> Result<VoteResponse> {
        // Leader 不会处理投票请求
        Ok(VoteResponse::new(core.state().current_term, false))
    }

    fn on_append_entries(&self, core: &mut RaftCore<impl Storage>, req: &AppendEntriesRequest) -> Result<AppendEntriesResponse> {
        // 如果收到更高任期的 AppendEntries，转换为 Follower
        if req.term > core.state().current_term {
            core.set_current_term(req.term)?;
            core.become_follower(req.leader_id);
            return Ok(AppendEntriesResponse::new(req.term, false));
        }
        Ok(AppendEntriesResponse::new(core.state().current_term, false))
    }
}

/// Candidate 处理器
pub struct CandidateHandler {
    /// 获得的票数
    votes_received: HashSet<NodeId>,
    /// 选举超时
    election_timeout: Duration,
    /// 开始选举时间
    election_started: Instant,
}

impl CandidateHandler {
    pub fn new(timeout: Duration) -> Self {
        Self {
            votes_received: HashSet::new(),
            election_timeout: timeout,
            election_started: Instant::now(),
        }
    }

    pub fn add_vote(&mut self, node_id: NodeId) {
        self.votes_received.insert(node_id);
    }

    pub fn has_majority(&self, cluster_size: usize) -> bool {
        self.votes_received.len() >= cluster_size / 2 + 1
    }

    pub fn is_election_timeout(&self) -> bool {
        self.election_started.elapsed() > self.election_timeout
    }

    pub fn restart_election(&mut self) {
        self.votes_received.clear();
        self.election_started = Instant::now();
    }
}

impl RoleHandler for CandidateHandler {
    fn role_type(&self) -> RoleType {
        RoleType::Candidate
    }

    fn on_election_timeout(&self, core: &mut RaftCore<impl Storage>) -> Result<()> {
        // 重新开始选举
        core.start_election()
    }

    fn on_heartbeat_timeout(&self, _core: &mut RaftCore<impl Storage>) -> Result<()> {
        Ok(())
    }

    fn on_vote_request(&self, core: &mut RaftCore<impl Storage>, req: &VoteRequest) -> Result<VoteResponse> {
        // Candidate 收到投票请求，可能是因为有其他 Candidate
        if req.term >= core.state().current_term {
            core.set_current_term(req.term)?;
            core.become_follower(req.candidate_id);
        }
        Ok(VoteResponse::new(core.state().current_term, false))
    }

    fn on_append_entries(&self, core: &mut RaftCore<impl Storage>, req: &AppendEntriesRequest) -> Result<AppendEntriesResponse> {
        if req.term >= core.state().current_term {
            core.become_follower(req.leader_id);
        }
        Ok(AppendEntriesResponse::new(core.state().current_term, false))
    }
}
```

---

### 3.2 存储层 `storage/`

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Storage 抽象层                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│                              ┌─────────────────┐                            │
│                              │   Storage Trait │                            │
│                              └────────┬────────┘                            │
│                                       │                                      │
│              ┌───────────────────────┼───────────────────────┐              │
│              │                       │                       │              │
│              ▼                       ▼                       ▼              │
│     ┌────────────────┐      ┌────────────────┐      ┌────────────────┐        │
│     │    RocksDB     │      │     Memory    │      │   MockStore    │        │
│     │   (生产环境)    │      │   (测试/嵌入)  │      │   (单元测试)    │        │
│     └────────────────┘      └────────────────┘      └────────────────┘        │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐     │
│  │                           StorageEngine                              │     │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌────────────┐ │     │
│  │  │   KvStore   │  │  UserStore   │  │  MetaStore   │  │  LogStore  │ │     │
│  │  └──────────────┘  └──────────────┘  └──────────────┘  └────────────┘ │     │
│  └─────────────────────────────────────────────────────────────────────┘     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 存储 Trait 定义

```rust
// src/storage/mod.rs

pub mod rocksdb;
pub mod memory;

use async_trait::async_trait;
use std::path::Path;

/// 存储错误类型
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("key not found: {0}")]
    NotFound(Vec<u8>),

    #[error("corruption: {0}")]
    Corruption(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("RocksDB error: {0}")]
    RocksDb(String),

    #[error("序列化错误: {0}")]
    Serialization(String),
}

/// Storage trait - 存储抽象
#[async_trait]
pub trait Storage: Send + Sync {
    /// 存储名称
    fn name(&self) -> &str;

    /// 打开或创建存储
    async fn open(path: impl AsRef<Path>) -> Result<Self, StorageError>
    where
        Self: Sized;

    /// 同步批量写入
    fn write_batch(&self, entries: Vec<(Box<[u8]>, Box<[u8]>)>) -> Result<(), StorageError>;

    /// 获取值
    fn get(&self, key: &[u8]) -> Result<Option<Box<[u8]>>, StorageError>;

    /// 设置值
    fn set(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError>;

    /// 删除值
    fn delete(&self, key: &[u8]) -> Result<(), StorageError>;

    /// 范围扫描
    fn scan(&self, start: &[u8], end: &[u8]) -> Result<Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)>>, StorageError>;

    /// 同步快照
    fn flush(&self) -> Result<(), StorageError>;

    /// 关闭存储
    fn close(&self) -> Result<(), StorageError>;

    /// 获取迭代器
    fn iterator(&self) -> Result<Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)>>, StorageError>;
}

/// RocksDB 存储实现
pub struct RocksDbStorage {
    db: rocksdb::DB,
}

#[async_trait]
impl Storage for RocksDbStorage {
    fn name(&self) -> &str {
        "rocksdb"
    }

    async fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let db = rocksdb::DB::open_default(path)
            .map_err(|e| StorageError::RocksDb(e.to_string()))?;
        Ok(Self { db })
    }

    fn write_batch(&self, entries: Vec<(Box<[u8]>, Box<[u8]>)>) -> Result<(), StorageError> {
        let mut batch = rocksdb::WriteBatch::default();
        for (key, value) in entries {
            batch.put(&key, &value);
        }
        self.db.write(batch)
            .map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    fn get(&self, key: &[u8]) -> Result<Option<Box<[u8]>>, StorageError> {
        match self.db.get(key) {
            Ok(Some(value)) => Ok(Some(value.into_boxed_slice())),
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::RocksDb(e.to_string())),
        }
    }

    fn set(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.db.put(key, value)
            .map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        self.db.delete(key)
            .map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    fn scan(&self, start: &[u8], end: &[u8]) -> Result<Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)>>, StorageError> {
        let iter = self.db.prefix_iterator(start)
            .map_while(|result| result.ok())
            .take_while(|(_, v)| v.as_ref() < end)
            .map(|(k, v)| (k.into_boxed_slice(), v.into_boxed_slice()));
        Ok(Box::new(iter))
    }

    fn flush(&self) -> Result<(), StorageError> {
        self.db.flush()
            .map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    fn close(&self) -> Result<(), StorageError> {
        // RocksDB 自动管理
        Ok(())
    }

    fn iterator(&self) -> Result<Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)>>, StorageError> {
        let iter = self.db.iterator(rocksdb::IteratorMode::Start)
            .map_while(|result| result.ok())
            .map(|(k, v)| (k.into_boxed_slice(), v.into_boxed_slice()));
        Ok(Box::new(iter))
    }
}

/// 内存存储实现 (用于测试)
pub struct MemoryStorage {
    data: RwLock<BTreeMap<Box<[u8]>, Box<[u8]>>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    fn name(&self) -> &str {
        "memory"
    }

    async fn open(_path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Ok(Self::new())
    }

    fn write_batch(&self, entries: Vec<(Box<[u8]>, Box<[u8]>)>) -> Result<(), StorageError> {
        let mut data = self.data.write();
        for (key, value) in entries {
            data.insert(key, value);
        }
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Box<[u8]>>, StorageError> {
        let data = self.data.read();
        Ok(data.get(key).cloned())
    }

    fn set(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let mut data = self.data.write();
        data.insert(key.into(), value.into());
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        let mut data = self.data.write();
        data.remove(key);
        Ok(())
    }

    fn scan(&self, start: &[u8], end: &[u8]) -> Result<Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)>>, StorageError> {
        let data = self.data.read();
        let iter = data.range(start.to_vec()..end.to_vec())
            .map(|(k, v)| (k.clone(), v.clone()));
        Ok(Box::new(iter))
    }

    fn flush(&self) -> Result<(), StorageError> {
        Ok(())
    }

    fn close(&self) -> Result<(), StorageError> {
        let mut data = self.data.write();
        data.clear();
        Ok(())
    }

    fn iterator(&self) -> Result<Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)>>, StorageError> {
        let data = self.data.read();
        let iter = data.iter().map(|(k, v)| (k.clone(), v.clone()));
        Ok(Box::new(iter))
    }
}
```

---

### 3.3 网络层 `transport/`

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Network Architecture                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│    ┌─────────────┐         ┌─────────────┐         ┌─────────────┐          │
│    │   Node A    │────────▶│   Node B    │◀────────│   Node C    │          │
│    └─────────────┘         └─────────────┘         └─────────────┘          │
│          │                       │                       │                  │
│          ▼                       ▼                       ▼                  │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐       │
│  │  EndpointMgr    │    │  EndpointMgr     │    │  EndpointMgr     │       │
│  └──────────────────┘    └──────────────────┘    └──────────────────┘       │
│          │                       │                       │                  │
│          ▼                       ▼                       ▼                  │
│  ┌──────────────────────────────────────────────────────────────────┐      │
│  │                    Connection Pool                                 │      │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐              │      │
│  │  │ Conn A→B │  │ Conn A→C│  │ Conn B→A │  │ Conn B→C │              │      │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘              │      │
│  └──────────────────────────────────────────────────────────────────┘      │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────┐      │
│  │                      Tokio Runtime                                 │      │
│  │  ┌──────────────────────────────────────────────────────────┐     │      │
│  │  │               AsyncRead + AsyncWrite                     │     │      │
│  │  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐            │     │      │
│  │  │  │ TCP    │ │ Framed │ │ Codec  │ │Router  │            │     │      │
│  │  │  │Stream  │ │Decoder │ │        │ │        │            │     │      │
│  │  │  └────────┘ └────────┘ └────────┘ └────────┘            │     │      │
│  │  └──────────────────────────────────────────────────────────┘     │      │
│  └──────────────────────────────────────────────────────────────────┘      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

```rust
// src/transport/mod.rs

pub mod tcp;
pub mod pool;
pub mod endpoint;

use crate::protocol::message::Message;
use crate::node::NodeId;
use std::io;

/// 网络错误
#[derive(Debug, Error)]
pub enum NetworkError {
    #[error("连接失败: {0}")]
    ConnectionFailed(String),

    #[error("连接关闭")]
    ConnectionClosed,

    #[error("超时")]
    Timeout,

    #[error("IO错误: {0}")]
    Io(#[from] io::Error),

    #[error("编码错误: {0}")]
    Encode(String),
}

/// 连接池 trait
pub trait ConnectionPool: Send + Sync {
    /// 获取到指定节点的连接
    async fn get(&self, node_id: NodeId) -> Result<Box<dyn Connection>, NetworkError>;

    /// 返回连接
    fn return_connection(&self, node_id: NodeId, conn: Box<dyn Connection>);

    /// 关闭所有连接
    fn close(&self) -> impl Future<Output = ()>;
}

/// 连接 trait
pub trait Connection: Send {
    /// 发送消息
    async fn send(&mut self, msg: &Message) -> Result<(), NetworkError>;

    /// 接收消息
    async fn recv(&mut self) -> Result<Message, NetworkError>;

    /// 关闭连接
    async fn close(self: Box<Self>) -> Result<(), NetworkError>;

    /// 获取远端节点ID
    fn peer_id(&self) -> NodeId;

    /// 是否已连接
    fn is_connected(&self) -> bool;
}

/// TCP 连接实现
pub struct TcpConnection {
    socket: tokio::net::TcpStream,
    framed: tokio_io_util::framed::Framed<
        tokio::net::TcpStream,
        crate::protocol::codec::MessageCodec,
    >,
    peer_id: NodeId,
}

impl TcpConnection {
    pub async fn connect(addr: &str, peer_id: NodeId) -> Result<Self, NetworkError> {
        let socket = tokio::net::TcpStream::connect(addr).await?;
        let framed = tokio_io_util::framed::Framed::new(
            socket,
            crate::protocol::codec::MessageCodec::default(),
        );
        Ok(Self { socket, framed, peer_id })
    }

    pub async fn from_socket(
        socket: tokio::net::TcpStream,
        peer_id: NodeId,
    ) -> Result<Self, NetworkError> {
        let framed = tokio_io_util::framed::Framed::new(
            socket,
            crate::protocol::codec::MessageCodec::default(),
        );
        Ok(Self { socket, framed, peer_id })
    }
}

impl Connection for TcpConnection {
    async fn send(&mut self, msg: &Message) -> Result<(), NetworkError> {
        self.framed.send(msg).await?;
        Ok(())
    }

    async fn recv(&mut self) -> Result<Message, NetworkError> {
        match self.framed.next().await {
            Some(Ok(msg)) => Ok(msg),
            Some(Err(e)) => Err(NetworkError::Io(e)),
            None => Err(NetworkError::ConnectionClosed),
        }
    }

    async fn close(self: Box<Self>) -> Result<(), NetworkError> {
        // Framed 自动处理
        Ok(())
    }

    fn peer_id(&self) -> NodeId {
        self.peer_id
    }

    fn is_connected(&self) -> bool {
        // 检查 socket 状态
        true
    }
}

/// 连接池实现
pub struct ConnectionPoolImpl {
    pool: RwLock<HashMap<NodeId, Vec<Box<dyn Connection>>>>,
    config: ConnectionPoolConfig,
}

impl ConnectionPoolImpl {
    pub fn new(config: ConnectionPoolConfig) -> Self {
        Self {
            pool: RwLock::new(HashMap::new()),
            config,
        }
    }

    async fn create_connection(&self, node_id: NodeId, addr: &str) -> Result<Box<dyn Connection>, NetworkError> {
        let conn = TcpConnection::connect(addr, node_id).await?;
        Ok(Box::new(conn))
    }
}

impl ConnectionPool for ConnectionPoolImpl {
    async fn get(&self, node_id: NodeId) -> Result<Box<dyn Connection>, NetworkError> {
        // 先尝试从池中获取
        {
            let mut pool = self.pool.write();
            if let Some(conns) = pool.get_mut(&node_id) {
                if let Some(conn) = conns.pop() {
                    if conn.is_connected() {
                        return Ok(conn);
                    }
                }
            }
        }

        // 需要创建新连接 (需要地址解析，这里简化处理)
        let addr = format!("{}:{}", node_id.ip(), node_id.port());
        self.create_connection(node_id, &addr).await
    }

    fn return_connection(&self, node_id: NodeId, conn: Box<dyn Connection>) {
        if conn.is_connected() {
            let mut pool = self.pool.write();
            pool.entry(node_id).or_default().push(conn);
        }
    }

    async fn close(&self) {
        let mut pool = self.pool.write();
        for (_node_id, conns) in pool.drain() {
            for conn in conns {
                let _ = conn.close().await;
            }
        }
    }
}
```

---

### 3.4 协议层 `protocol/`

```rust
// src/protocol/mod.rs

pub mod message;
pub mod codec;

use serde::{Serialize, Deserialize};

/// 消息类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum MessageType {
    // Raft 协议消息
    Propose = 0x01,
    AppendEntries = 0x02,
    AppendEntriesResponse = 0x03,
    VoteRequest = 0x04,
    VoteResponse = 0x05,
    Heartbeat = 0x06,
    HeartbeatResponse = 0x07,
    Snapshot = 0x08,
    SnapshotResponse = 0x09,

    // 客户端消息
    Get = 0x10,
    Put = 0x11,
    Delete = 0x12,
    Scan = 0x13,
    Lock = 0x14,
    Unlock = 0x15,
    Watch = 0x16,
    Unwatch = 0x17,
    Login = 0x18,
    Logout = 0x19,

    // 内部消息
    Timeout = 0xF0,
}

/// 消息信封
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEnvelope {
    /// 源节点ID
    pub from: NodeId,
    /// 目标节点ID
    pub to: NodeId,
    /// 消息类型
    pub msg_type: MessageType,
    /// 消息体
    pub payload: Vec<u8>,
    /// 时间戳
    pub timestamp: u64,
}

impl MessageEnvelope {
    pub fn new(from: NodeId, to: NodeId, msg_type: MessageType, payload: Vec<u8>) -> Self {
        Self {
            from,
            to,
            msg_type,
            payload,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }
}

/// 节点ID
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(u64);

impl NodeId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// 统一消息类型
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum Message {
    // === Raft 消息 ===

    AppendEntries(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    Heartbeat(HeartbeatRequest),
    HeartbeatResponse(HeartbeatResponse),
    Snapshot(SnapshotRequest),
    SnapshotResponse(SnapshotResponse),

    // === 客户端请求 ===

    Get(GetRequest),
    Put(PutRequest),
    Delete(DeleteRequest),
    Scan(ScanRequest),
    Lock(LockRequest),
    Unlock(UnlockRequest),
    Watch(WatchRequest),
    Login(LoginRequest),
    Logout(LogoutRequest),
}

/// Raft AppendEntries 请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<Entry>,
    pub leader_commit: LogIndex,
}

/// Raft AppendEntries 响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
    pub match_index: LogIndex,
    pub last_index: LogIndex,
}

/// Raft VoteRequest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

/// Raft VoteResponse
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

/// 日志条目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub index: LogIndex,
    pub term: Term,
    pub data: EntryData,
}

/// 日志索引
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LogIndex(u64);

impl LogIndex {
    pub fn new(val: u64) -> Self { Self(val) }
    pub fn val(&self) -> u64 { self.0 }
    pub fn next(&self) -> Self { Self(self.0 + 1) }
}

/// 任期号
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Term(u64);

impl Term {
    pub fn new(val: u64) -> Self { Self(val) }
    pub fn val(&self) -> u64 { self.0 }
}
```

---

### 3.5 业务功能层 `features/`

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Business Features                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐     │
│  │                          LockManager                                  │     │
│  │  ┌─────────────────────────────────────────────────────────────┐    │     │
│  │  │  Locks: { key -> (owner_session, expiration) }              │    │     │
│  │  │  WaitQueue: { key -> Vec<session_id> }                      │    │     │
│  │  └─────────────────────────────────────────────────────────────┘    │     │
│  │                                                                      │     │
│  │  Methods:                                                            │     │
│  │  - lock(key, session) -> Result<LockToken>                          │     │
│  │  - unlock(key, session) -> Result<()>                               │     │
│  │  - try_lock(key, session) -> Result<bool>                           │     │
│  │  - extend_lock(key, session) -> Result<()>                         │     │
│  └─────────────────────────────────────────────────────────────────────┘     │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐     │
│  │                          WatchManager                                 │     │
│  │  ┌─────────────────────────────────────────────────────────────┐    │     │
│  │  │  Watchers: { key -> Vec<(session_id, callback_channel)> }  │    │     │
│  │  │  PendingEvents: Vec<WatchEvent>                             │    │     │
│  │  └─────────────────────────────────────────────────────────────┘    │     │
│  │                                                                      │     │
│  │  Methods:                                                            │     │
│  │  - watch(key, session) -> WatchId                                   │     │
│  │  - unwatch(key, watch_id)                                           │     │
│  │  - trigger(key, event) -> notify all watchers                       │     │
│  └─────────────────────────────────────────────────────────────────────┘     │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐     │
│  │                          SessionManager                              │     │
│  │  ┌─────────────────────────────────────────────────────────────┐    │     │
│  │  │  Sessions: { session_id -> SessionInfo }                    │    │     │
│  │  │  UserSessions: { user -> Vec<session_id> }                  │    │     │
│  │  └─────────────────────────────────────────────────────────────┘    │     │
│  │                                                                      │     │
│  │  Methods:                                                            │     │
│  │  - create_session(user) -> Session                                  │     │
│  │  - refresh_session(session_id)                                       │     │
│  │  - expire_session(session_id)                                       │     │
│  │  - validate_session(session_id) -> bool                             │     │
│  └─────────────────────────────────────────────────────────────────────┘     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 锁管理器

```rust
// src/features/lock/mod.rs

pub mod manager;
pub mod types;

use crate::features::session::SessionId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};

/// 锁管理器
pub struct LockManager {
    /// 锁状态: key -> LockState
    locks: RwLock<HashMap<Vec<u8>, LockState>>,
    /// 锁等待队列: key -> Vec<等待者>
    wait_queues: RwLock<HashMap<Vec<u8>, Vec<LockWaiter>>>,
    /// 事件发送器
    event_sender: mpsc::Sender<LockEvent>,
}

/// 锁状态
struct LockState {
    /// 持有者会话
    owner: SessionId,
    /// 过期时间
    expires_at: std::time::Instant,
    /// 锁版本号
    version: u64,
}

/// 等待者
struct LockWaiter {
    /// 会话ID
    session: SessionId,
    /// 期望的版本号
    expect_version: u64,
    /// 唤醒信号
    wake_tx: tokio::sync::oneshot::Sender<Result<(), LockError>>,
}

/// 锁令牌
#[derive(Debug, Clone)]
pub struct LockToken {
    pub key: Vec<u8>,
    pub session_id: SessionId,
    pub version: u64,
    pub expires_at: std::time::Instant,
}

/// 锁事件
#[derive(Debug)]
pub enum LockEvent {
    Acquired { key: Vec<u8>, token: LockToken },
    Released { key: Vec<u8>, session_id: SessionId },
    Timeout { key: Vec<u8> },
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
            wait_queues: RwLock::new(HashMap::new()),
            event_sender: mpsc::channel(1000).0,
        }
    }

    /// 尝试获取锁
    pub async fn try_lock(&self, key: Vec<u8>, session: SessionId, ttl: std::time::Duration) -> Result<LockToken, LockError> {
        let mut locks = self.locks.write().await;

        match locks.get_mut(&key) {
            Some(state) => {
                // 锁已被占用
                if state.expires_at > std::time::Instant::now() && state.owner != session {
                    return Err(LockError::AlreadyLocked {
                        key: key.clone(),
                        holder: state.owner,
                    });
                }
                // 锁已过期或被同一会话持有，更新状态
                state.owner = session.clone();
                state.expires_at = std::time::Instant::now() + ttl;
                state.version += 1;

                Ok(LockToken {
                    key: key.clone(),
                    session_id: session,
                    version: state.version,
                    expires_at: state.expires_at,
                })
            }
            None => {
                // 创建新锁
                let version = 1;
                let expires_at = std::time::Instant::now() + ttl;
                locks.insert(key.clone(), LockState {
                    owner: session.clone(),
                    expires_at,
                    version,
                });

                Ok(LockToken {
                    key,
                    session_id: session,
                    version,
                    expires_at,
                })
            }
        }
    }

    /// 释放锁
    pub async fn unlock(&self, key: &[u8], session: SessionId) -> Result<(), LockError> {
        let mut locks = self.locks.write().await;
        let mut wait_queues = self.wait_queues.write().await;

        match locks.get_mut(key) {
            Some(state) if state.owner == session => {
                // 检查等待队列
                if let Some(waiters) = wait_queues.get_mut(key) {
                    if let Some(waiter) = waiters.pop(0) {
                        // 授予下一个等待者
                        state.owner = waiter.session.clone();
                        state.version += 1;
                        state.expires_at = std::time::Instant::now() + std::time::Duration::from_secs(60);

                        let _ = waiter.wake_tx.send(Ok(()));
                    }
                }

                // 如果没有等待者，删除锁
                if wait_queues.get(key).map_or(true, |q| q.is_empty()) {
                    locks.remove(key);
                    wait_queues.remove(key);
                }

                Ok(())
            }
            Some(_) => Err(LockError::NotOwner),
            None => Err(LockError::NotLocked),
        }
    }

    /// 延长锁的 TTL
    pub async fn extend(&self, key: &[u8], session: SessionId, ttl: std::time::Duration) -> Result<(), LockError> {
        let mut locks = self.locks.write().await;

        match locks.get_mut(key) {
            Some(state) if state.owner == session => {
                state.expires_at = std::time::Instant::now() + ttl;
                Ok(())
            }
            Some(_) => Err(LockError::NotOwner),
            None => Err(LockError::NotLocked),
        }
    }
}

/// 锁错误
#[derive(Debug, Error)]
pub enum LockError {
    #[error("锁已被占用: key={key}, holder={holder}")]
    AlreadyLocked { key: Vec<u8>, holder: SessionId },

    #[error("不是锁的持有者")]
    NotOwner,

    #[error("锁未被持有")]
    NotLocked,

    #[error("等待超时")]
    WaitTimeout,
}
```

#### Watch 管理器

```rust
// src/features/watch/mod.rs

use crate::features::session::SessionId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc, oneshot};
use uuid::Uuid;

/// Watch 事件
#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub event_type: WatchEventType,
}

#[derive(Debug, Clone)]
pub enum WatchEventType {
    Put,
    Delete,
    Expire,
}

/// Watch 订阅者
struct Watcher {
    id: WatchId,
    session: SessionId,
    receiver: mpsc::Receiver<WatchEvent>,
}

/// Watch ID
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WatchId(Uuid);

impl WatchId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// Watch 管理器
pub struct WatchManager {
    /// 订阅映射: key -> Vec<Watcher>
    subscriptions: RwLock<HashMap<Vec<u8>, HashMap<WatchId, Watcher>>>,
    /// 会话订阅: session -> Vec<WatchId>
    session_watches: RwLock<HashMap<SessionId, Vec<WatchId>>>,
}

impl WatchManager {
    pub fn new() -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            session_watches: RwLock::new(HashMap::new()),
        }
    }

    /// 订阅某个 key
    pub async fn watch(
        &self,
        key: Vec<u8>,
        session: SessionId,
    ) -> (WatchId, mpsc::Receiver<WatchEvent>) {
        let (tx, rx) = mpsc::channel(100);
        let watch_id = WatchId::new();

        // 创建 watcher
        let watcher = Watcher {
            id: watch_id,
            session: session.clone(),
            receiver: rx,
        };

        // 添加到订阅映射
        let mut subs = self.subscriptions.write().await;
        subs.entry(key.clone())
            .or_insert_with(HashMap::new)
            .insert(watch_id, watcher);

        // 添加到会话映射
        let mut session_watches = self.session_watches.write().await;
        session_watches
            .entry(session)
            .or_insert_with(Vec::new)
            .push(watch_id);

        (watch_id, tx)
    }

    /// 取消订阅
    pub async fn unwatch(&self, key: &[u8], watch_id: WatchId) -> bool {
        let mut subs = self.subscriptions.write().await;

        if let Some(watchers) = subs.get_mut(key) {
            if let Some(watcher) = watchers.remove(&watch_id) {
                // 从会话映射中移除
                let mut session_watches = self.session_watches.write().await;
                if let Some(ids) = session_watches.get_mut(&watcher.session) {
                    ids.retain(|&id| id != watch_id);
                }
                return true;
            }
        }
        false
    }

    /// 触发事件
    pub async fn trigger(&self, event: WatchEvent) {
        let subs = self.subscriptions.read().await;

        // 精确匹配
        if let Some(watchers) = subs.get(&event.key) {
            for (_, watcher) in watchers {
                let _ = watcher.receiver.sender().try_send(event.clone());
            }
        }

        // 前缀匹配 (watch 父目录)
        for (key, watchers) in subs.iter() {
            if event.key.starts_with(key) && !event.key.is_empty() {
                for (_, watcher) in watchers {
                    let _ = watcher.receiver.sender().try_send(event.clone());
                }
            }
        }
    }

    /// 取消会话的所有订阅
    pub async fn unwatch_session(&self, session: &SessionId) {
        let mut session_watches = self.session_watches.write().await;
        let mut subs = self.subscriptions.write().await;

        if let Some(watch_ids) = session_watches.remove(session) {
            for watch_id in watch_ids {
                for watchers in subs.values_mut() {
                    watchers.remove(&watch_id);
                }
            }
        }

        // 清理空的订阅
        subs.retain(|_, watchers| !watchers.is_empty());
    }
}
```

---

### 3.6 服务端 `server/`

```rust
// src/server/mod.rs

pub mod node;
pub mod handler;
pub mod coordinator;

use crate::raft::{RaftCore, RaftConfig, RaftStorage};
use crate::features::{LockManager, WatchManager, SessionManager};
use crate::storage::{Storage, RocksDbStorage};
use crate::transport::{ConnectionPool, ConnectionPoolImpl};
use std::sync::Arc;
use tokio::sync::RwLock;

/// 服务节点
pub struct ServerNode {
    /// 节点ID
    node_id: NodeId,
    /// Raft 核心
    raft: Arc<RaftCore<RocksDbStorage>>,
    /// 锁管理器
    lock_manager: Arc<LockManager>,
    /// Watch 管理器
    watch_manager: Arc<WatchManager>,
    /// 会话管理器
    session_manager: Arc<SessionManager>,
    /// 连接池
    connection_pool: Arc<ConnectionPoolImpl>,
    /// 协调器
    coordinator: Coordinator,
}

/// 请求处理器
pub struct RequestHandler {
    node: Arc<ServerNode>,
}

impl RequestHandler {
    pub async fn handle_put(&self, req: PutRequest) -> Result<PutResponse, ServerError> {
        // 1. 验证会话
        let session = self.node.session_manager.validate(&req.session_id)
            .ok_or(ServerError::InvalidSession)?;

        // 2. 提交到 Raft
        let entry = EntryData::Data {
            key: req.key.clone(),
            value: req.value,
        };
        let propose_result = self.node.raft.propose(entry).await?;

        Ok(PutResponse {
            success: true,
            index: propose_result.index,
            term: propose_result.term,
        })
    }

    pub async fn handle_get(&self, req: GetRequest) -> Result<GetResponse, ServerError> {
        // 读请求可以直接从状态机读取
        let state = self.node.raft.state_machine().read().await;

        match state.get(&req.key) {
            Some(value) => Ok(GetResponse {
                found: true,
                value,
                ..Default::default()
            }),
            None => Ok(GetResponse {
                found: false,
                ..Default::default()
            }),
        }
    }

    pub async fn handle_lock(&self, req: LockRequest) -> Result<LockResponse, ServerError> {
        let session = self.node.session_manager.validate(&req.session_id)
            .ok_or(ServerError::InvalidSession)?;

        match self.node.lock_manager.try_lock(
            req.key.clone(),
            session.id.clone(),
            std::time::Duration::from_secs(req.ttl_seconds),
        ).await {
            Ok(token) => Ok(LockResponse {
                success: true,
                token: Some(LockTokenResponse {
                    version: token.version,
                    expires_at: token.expires_at.as_secs() as u64,
                }),
            }),
            Err(e) => Ok(LockResponse {
                success: false,
                error: Some(e.to_string()),
                ..Default::default()
            }),
        }
    }

    pub async fn handle_watch(&self, req: WatchRequest) -> Result<WatchResponse, ServerError> {
        let (watch_id, mut receiver) = self.node.watch_manager
            .watch(req.key.clone(), req.session_id)
            .await;

        // 在后台任务中等待事件
        tokio::spawn(async move {
            if let Some(event) = receiver.recv().await {
                // 处理事件
            }
        });

        Ok(WatchResponse {
            watch_id: Some(watch_id.0.to_string()),
            ..Default::default()
        })
    }
}

/// 协调器 - 协调多个组件
pub struct Coordinator {
    /// 批量应用通道
    apply_channel: mpsc::Sender<ApplyRequest>,
}

impl Coordinator {
    pub fn new(config: CoordinatorConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.apply_channel_size);
        Self {
            apply_channel: tx,
        }
    }

    /// 启动协调器
    pub async fn run(&self) {
        // 启动各种协调任务
    }
}
```

---

### 3.7 客户端 SDK `api/`

```rust
// src/api/mod.rs

pub mod client;
pub mod builder;
pub mod error;
pub mod types;

use crate::protocol::{Message, MessageType, NodeId};
use crate::transport::{ConnectionPool, ConnectionPoolImpl};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};

/// 客户端错误
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("连接失败: {0}")]
    ConnectionFailed(String),

    #[error("超时")]
    Timeout,

    #[error("集群不可用")]
    ClusterUnavailable,

    #[error("节点不是 Leader: leader={leader}")]
    NotLeader { leader: NodeId },

    #[error("键不存在: {0}")]
    KeyNotFound(Vec<u8>),

    #[error("锁冲突: {0}")]
    LockConflict(String),

    #[error("认证失败: {0}")]
    AuthFailed(String),

    #[error("Raft 错误: {0}")]
    Raft(String),
}

/// Nexus 客户端
pub struct NexusClient {
    /// 配置
    config: ClientConfig,
    /// 连接池
    connection_pool: Arc<ConnectionPoolImpl>,
    /// 当前 Leader
    leader: RwLock<Option<NodeId>>,
    /// 会话ID
    session_id: RwLock<Option<String>>,
    /// 心跳任务
    heartbeat_task: RwLock<Option<tokio::task::JoinHandle<()>>>,
}

/// 客户端配置
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// 服务器地址列表
    pub servers: Vec<String>,
    /// 连接超时
    pub connect_timeout: Duration,
    /// 请求超时
    pub request_timeout: Duration,
    /// 心跳间隔
    pub heartbeat_interval: Duration,
    /// 重试次数
    pub retry_count: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            servers: Vec::new(),
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(30),
            retry_count: 3,
        }
    }
}

impl NexusClient {
    /// 创建客户端
    pub async fn new(config: ClientConfig) -> Result<Self, ClientError> {
        let connection_pool = ConnectionPoolImpl::new(ConnectionPoolConfig::default());

        Ok(Self {
            config,
            connection_pool: Arc::new(connection_pool),
            leader: RwLock::new(None),
            session_id: RwLock::new(None),
            heartbeat_task: RwLock::new(None),
        })
    }

    /// 连接集群
    pub async fn connect(&self) -> Result<(), ClientError> {
        // 发现所有节点
        for server in &self.config.servers {
            if let Ok(conn) = self.connection_pool.get(NodeId::new(0)).await {
                // 获取集群信息
                break;
            }
        }
        Ok(())
    }

    /// PUT 操作
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), ClientError> {
        self.ensure_leader().await?;

        let leader = self.leader.read().await;
        let mut conn = self.connection_pool.get(leader.unwrap()).await?;

        let req = PutRequest {
            key: key.clone(),
            value,
            session_id: self.session_id.read().await.clone(),
            uuid: Uuid::new_v4().to_string(),
        };

        let resp = conn.send_and_receive(MessageType::Put, &req).await?;

        match resp {
            Ok(PutResponse { success: true, .. }) => Ok(()),
            Ok(resp) => Err(ClientError::Raft(format!("put failed: {:?}", resp))),
            Err(e) => Err(e),
        }
    }

    /// GET 操作
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        self.ensure_leader().await?;

        let leader = self.leader.read().await;
        let mut conn = self.connection_pool.get(leader.unwrap()).await?;

        let req = GetRequest {
            key: key.to_vec(),
            session_id: self.session_id.read().await.clone(),
            uuid: Uuid::new_v4().to_string(),
        };

        let resp = conn.send_and_receive(MessageType::Get, &req).await?;

        match resp {
            Ok(GetResponse { found: true, value, .. }) => Ok(Some(value)),
            Ok(GetResponse { found: false, .. }) => Ok(None),
            Ok(resp) => Err(ClientError::Raft(format!("get failed: {:?}", resp))),
            Err(e) => Err(e),
        }
    }

    /// 分布式锁
    pub async fn lock(&self, key: Vec<u8>) -> Result<LockGuard, ClientError> {
        let token = self.do_lock(key.clone(), Duration::from_secs(60)).await?;

        Ok(LockGuard {
            client: self.clone(),
            key,
            token,
        })
    }

    /// 内部锁实现
    async fn do_lock(&self, key: Vec<u8>, ttl: Duration) -> Result<LockToken, ClientError> {
        for _ in 0..self.config.retry_count {
            self.ensure_leader().await?;

            let leader = self.leader.read().await;
            let mut conn = self.connection_pool.get(leader.unwrap()).await?;

            let req = LockRequest {
                key: key.clone(),
                session_id: self.session_id.read().await.clone().unwrap_or_default(),
                ttl_seconds: ttl.as_secs() as u32,
            };

            match conn.send_and_receive(MessageType::Lock, &req).await? {
                Ok(LockResponse { success: true, token: Some(t), .. }) => {
                    return Ok(LockToken {
                        version: t.version,
                        expires_at: std::time::Instant::now() + Duration::from_secs(t.expires_at),
                    });
                }
                Ok(LockResponse { success: false, error: Some(e), .. }) => {
                    return Err(ClientError::LockConflict(e));
                }
                Err(e) => {
                    // 刷新 leader
                    *self.leader.write().await = None;
                    continue;
                }
                _ => continue,
            }
        }

        Err(ClientError::LockConflict("failed to acquire lock".into()))
    }

    /// 确保有有效的 leader
    async fn ensure_leader(&self) -> Result<(), ClientError> {
        if self.leader.read().await.is_some() {
            return Ok(());
        }

        // 遍历服务器列表发现 leader
        for server in &self.config.servers {
            // 尝试连接并获取状态
            // ...
        }

        Err(ClientError::ClusterUnavailable)
    }

    /// 登录
    pub async fn login(&self, username: &str, password: &str) -> Result<(), ClientError> {
        let session_id = Uuid::new_v4().to_string();

        // 选择任一服务器发送登录请求
        for server in &self.config.servers {
            let mut conn = self.connection_pool.get(NodeId::new(0)).await?;

            let req = LoginRequest {
                username: username.to_string(),
                password: password.to_string(),
            };

            match conn.send_and_receive(MessageType::Login, &req).await? {
                Ok(LoginResponse { success: true, uuid, .. }) => {
                    *self.session_id.write().await = uuid.or(Some(session_id));
                    self.start_heartbeat().await;
                    return Ok(());
                }
                Ok(LoginResponse { success: false, error: Some(e), .. }) => {
                    return Err(ClientError::AuthFailed(e));
                }
                Err(e) => continue,
                _ => continue,
            }
        }

        Err(ClientError::ClusterUnavailable)
    }
}

/// 锁守护 - RAII 模式的锁
pub struct LockGuard {
    client: NexusClient,
    key: Vec<u8>,
    token: LockToken,
}

impl LockGuard {
    /// 释放锁
    pub async fn unlock(self) -> Result<(), ClientError> {
        let mut conn = self.client.connection_pool.get(
            *self.client.leader.read().await
        ).await?;

        let req = UnlockRequest {
            key: self.key.clone(),
            session_id: self.client.session_id.read().await.clone().unwrap_or_default(),
            version: self.token.version,
        };

        conn.send_and_receive(MessageType::Unlock, &req).await?;
        Ok(())
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        // 异步释放锁 (不阻塞)
        let client = self.client.clone();
        let key = self.key.clone();
        tokio::spawn(async move {
            let _ = client.unlock_internal(&key).await;
        });
    }
}
```

---

## 4. Cargo.toml 依赖设计

```toml
[package]
name = "nexus"
version = "0.1.0"
edition = "2021"
authors = ["Nexus Team"]
description = "High available distributed key-value store based on Raft"

[lib]
name = "nexus"
path = "src/lib.rs"

[[bin]]
name = "nexus-server"
path = "src/bin/server.rs"

[[bin]]
name = "nexus-cli"
path = "src/bin/client.rs"

[dependencies]
# === 异步运行时 ===
tokio = { version = "1.35", features = ["full"] }
tokio-rustls = "0.24"

# === 网络 ===
tonic = "0.10"                    # gRPC (替代 sofa-pbrpc)
prost = "0.12"                    # Protobuf 序列化
bytes = "1.5"                     # 零拷贝字节处理

# === 存储 ===
rocksdb = "0.21"                  # RocksDB
snappy = "0.5"                    # 压缩

# === 序列化 ===
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
rmp-serde = "1.1"                # MessagePack

# === 异步抽象 ===
async-trait = "0.1"
futures = "0.3"

# === 工具 ===
uuid = { version = "1.6", features = ["v4", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
thiserror = "1.0"
anyhow = "1.0"
parking_lot = "0.12"             # 更快的锁
dashmap = "5.5"                  # 并发 HashMap
tracing = "0.1"
tracing-subscriber = "0.3"

# === 配置 ===
config = "0.14"
clap = { version = "4.4", features = ["derive"] }

# === 指标 ===
metrics = "0.21"
prometheus-client = "0.22"

# === 限流/熔断 ===
governor = "0.6"
ratelimit = "0.9"

[dev-dependencies]
# === 测试 ===
tokio-test = "0.4"
proptest = "1.4"
criterion = { version = "0.5", features = ["html_reports"] }

# === 模糊测试 ===
libfuzzer-sys = "0.4"

[profile.release]
opt-level = 3
lto = true
codegen-units = 1

[profile.bench]
inherits = "release"
```

---

## 5. 错误处理策略

```rust
// src/api/error.rs

use thiserror::Error;
use crate::raft::{RaftError, RoleType};
use crate::storage::StorageError;
use crate::transport::NetworkError;

/// 统一错误类型
#[derive(Error, Debug)]
pub enum Error {
    // === Raft 错误 ===
    #[error("Raft 错误: {0}")]
    Raft(#[from] RaftError),

    #[error("节点角色错误: 当前={current}, 期望={expected}")]
    RoleMismatch { current: RoleType, expected: RoleType },

    #[error("日志索引错误: index={index}, 原因={reason}")]
    LogIndexError { index: u64, reason: String },

    // === 存储错误 ===
    #[error("存储错误: {0}")]
    Storage(#[from] StorageError),

    #[error("键不存在: {0:?}")]
    KeyNotFound(Vec<u8>),

    // === 网络错误 ===
    #[error("网络错误: {0}")]
    Network(#[from] NetworkError),

    #[error("连接超时")]
    Timeout,

    #[error("集群不可用")]
    ClusterUnavailable,

    // === 业务错误 ===
    #[error("认证失败: {0}")]
    AuthFailed(String),

    #[error("权限不足")]
    PermissionDenied,

    #[error("锁冲突: {0}")]
    LockConflict(String),

    #[error("会话过期")]
    SessionExpired,

    #[error("用户已存在: {0}")]
    UserExists(String),

    #[error("用户不存在: {0}")]
    UserNotFound(String),

    // === 内部错误 ===
    #[error("编码错误: {0}")]
    Encode(String),

    #[error("解码错误: {0}")]
    Decode(String),

    #[error("未知错误: {0}")]
    Unknown(String),
}

/// 结果类型别名
pub type Result<T> = std::result::Result<T, Error>;
```

---

## 6. 配置设计

```rust
// src/util/config.rs

use serde::{Deserialize, Serialize};
use config::{Config, ConfigError, File};

/// Raft 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    /// 节点 ID
    pub node_id: u64,
    /// 集群节点列表
    pub cluster: Vec<ClusterNode>,
    /// 选举超时最小值 (ms)
    pub election_timeout_min: u64,
    /// 选举超时最大值 (ms)
    pub election_timeout_max: u64,
    /// 心跳间隔 (ms)
    pub heartbeat_interval: u64,
    /// 日志复制批量大小
    pub replication_batch_size: usize,
    /// 最大登录尝试次数
    pub max_inflight_logs: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            cluster: Vec::new(),
            election_timeout_min: 500,
            election_timeout_max: 1000,
            heartbeat_interval: 150,
            replication_batch_size: 1000,
            max_inflight_logs: 256,
        }
    }
}

/// 集群节点
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNode {
    pub id: u64,
    pub host: String,
    pub port: u16,
}

/// 服务器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// 数据目录
    pub data_dir: String,
    /// Raft 配置
    pub raft: RaftConfig,
    /// 存储配置
    pub storage: StorageConfig,
    /// 网络配置
    pub network: NetworkConfig,
    /// 会话配置
    pub session: SessionConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            data_dir: "./data".to_string(),
            raft: RaftConfig::default(),
            storage: StorageConfig::default(),
            network: NetworkConfig::default(),
            session: SessionConfig::default(),
        }
    }
}

impl ServerConfig {
    pub fn load(path: &str) -> Result<Self, ConfigError> {
        let config = Config::builder()
            .add_source(File::with_name(path))
            .build()?;

        config.try_deserialize()
    }
}

/// 存储配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// 存储引擎类型
    pub engine: String,
    /// RocksDB 选项
    pub rocksdb: RocksDbConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksDbConfig {
    pub write_buffer_size: usize,
    pub max_write_buffer_number: i32,
    pub max_background_jobs: i32,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            engine: "rocksdb".to_string(),
            rocksdb: RocksDbConfig {
                write_buffer_size: 64 << 20,
                max_write_buffer_number: 4,
                max_background_jobs: 4,
            },
        }
    }
}

/// 网络配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub bind_host: String,
    pub bind_port: u16,
    pub max_connections: usize,
    pub recv_buffer_size: usize,
    pub send_buffer_size: usize,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            bind_host: "0.0.0.0".to_string(),
            bind_port: 8080,
            max_connections: 10000,
            recv_buffer_size: 64 << 10,
            send_buffer_size: 64 << 10,
        }
    }
}

/// 会话配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionConfig {
    /// 会话超时时间 (秒)
    pub timeout: u64,
    /// 清理间隔 (秒)
    pub cleanup_interval: u64,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            timeout: 600,
            cleanup_interval: 60,
        }
    }
}
```

---

## 7. Rust 重写 vs C++ 对比

| 维度 | C++ 版本 | Rust 重写 | 优势 |
|------|----------|-----------|------|
| **并发安全** | 手动加锁 + Mutex | 所有权系统 | 编译期保证，无数据竞争 |
| **内存安全** | 手动管理 | RAII + 生命周期 | 无悬垂指针，零泄漏 |
| **错误处理** | 异常机制 | Result<T, E> | 类型安全，无异常泄漏 |
| **异步编程** | 回调/Promise | async/await | 同步风格写异步代码 |
| **模块化** | 头文件包含 | Cargo 模块 | 细粒度依赖管理 |
| **测试** | gtest | 内置测试框架 | 更易用的 Mock |
| **性能** | 高 | 接近零成本抽象 | Rust 不输 C++ |

---

## 8. 实施建议

### 阶段一：核心框架 (Month 1-2)
```
1. 项目脚手架搭建 (Cargo.toml, 目录结构)
2. Raft 核心协议实现
3. 存储抽象层
4. 基础网络通信
```

### 阶段二：功能实现 (Month 3-4)
```
1. KV 状态机
2. 锁管理器
3. Watch 机制
4. 会话管理
```

### 阶段三：SDK 和工具 (Month 5)
```
1. 客户端 SDK
2. CLI 工具
3. 配置管理
4. 监控指标
```

### 阶段四：生产就绪 (Month 6)
```
1. 性能调优
2. 集成测试
3. 文档完善
4. 发布准备
```

---

这个架构设计充分利用了 Rust 的语言特性，提供了编译期安全保障，同时保持了高性能和高可用性。
