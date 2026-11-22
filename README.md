# distributed-fs-metadata-lock-recovery-planner
恢复分布式文件系统元数据锁和未完成命名操作

## 概述

元数据服务重启后，部分 inode 锁、目录项租约和 rename 操作可能只完成了一半。
本工具根据抽象输入计算恢复计划：哪些操作可以回放（replay）、哪些必须撤销
（undo）、哪些必须等待（blocked）。

**注意：本工具只处理抽象日志与状态快照，不读写任何真实文件系统。**

## 输入

`recovery_planner.RecoveryInput`：

- `sessions`：客户端会话（client_id、session_id、epoch、是否已重连）
- `locks`：inode 锁 / 目录项租约（资源、持有者、过期时间）
- `log`：操作日志（prepare / commit / abort，带序号与时间戳；重复记录自动去重）
- `rename_deps`：rename 对源/目标父目录持久化版本的依赖
- `parent_states`：父目录的持久化状态（版本号）
- `now`：当前时间，用于判定租约超时

## 恢复规则

- 锁恢复按资源依赖拓扑排序：共享资源的操作按日志序号串行化；rename 还依赖
  所有触碰其源/目标父目录的操作，交叉 rename 形成环时整体阻断。
- 已失去租约的旧客户端不能继续提交：租约过期后到达的 commit 被拒绝，其
  prepare-only 操作一律撤销。
- 目录 rename 只有在源和目标父目录的持久化版本与 prepare 时一致时才能回放，
  否则阻断（`parent_state_mismatch`）。
- 租约仍有效的客户端，其未完成操作与未释放锁必须等待（不得提前回收，否则旧
  客户端可能继续写入）。

## 输出

`RecoveryPlan`：

- `recovery_order`：按依赖排序的回放/撤销动作列表
- `orphaned_locks`：租约已过期、可安全回收的孤立锁
- `blocked`：阻断项及原因（租约未过期、父目录不一致、依赖环、等待租约超时）

## 用法

```bash
# 从 JSON 快照生成恢复计划（也可通过 stdin 传入）
python3 recovery_planner.py snapshot.json
```

或在代码中调用：

```python
from recovery_planner import RecoveryInput, plan_recovery
plan = plan_recovery(RecoveryInput(...))
```

## 测试

```bash
python -m unittest
```

测试覆盖 rename 中断、租约过期、父目录不一致和重复日志等场景。
兼容 Python 3.8+。
