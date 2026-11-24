# message-queue-delayed-index-recovery-planner
恢复延迟消息索引、租约和重复到期项

## 背景

延迟消息的 not-before 时间保存在一个**独立索引**中，索引进程到期后把消息投递到原队列。
进程崩溃时，部分消息可能已经进入原队列，但索引里的旧项还没清理；恢复时如果盲目重投，
就会产生重复投递。本项目用 Python 3.8.2 兼容的代码，根据崩溃后仍存活的状态给出恢复计划。

> 注意：本仓库中的原队列、投递确认和租约存储**全部是进程内模拟**（见
> `delayed_index/simqueue.py`），不连接任何真实消息中间件或磁盘。

## 输入

`RecoveryInput`（`delayed_index/model.py`）接收：

- 索引项 `IndexEntry`：消息 ID、not-before 时间、原队列 offset、索引 position。
- 已投递确认 `DeliveryRecord`：只有原队列入队被持久确认后才会出现。
- 扫描游标 `scan_cursor` 及其写入时的租约 fencing token。
- 索引提交点 `index_commit_point`。
- 租约 `Lease`（持有者、fencing token、过期时间）和当前时间 `now`。

## 判定规则

每个索引项得到三种结论之一（`Verdict`）：

- `DELETE`：入队已确认且 offset 一致，索引项是残留垃圾，可直接删除。
- `REBUILD`：已到期但无法确认是否入过队，需要幂等地重建/重投；位于崩溃扫描窗口内的项带重复风险。
- `WAIT`：not-before 还在未来，继续保留在剩余延迟集中等待。

两条硬约束：

1. **索引提交不能越过实际投递确认**：有效提交点被钳制为「已确认投递的连续 position 前缀」，
   即使后面的消息已确认，中间有空缺也不能跳过。
2. **旧租约不能再推进扫描游标**：租约过期，或游标 fencing token 与当前租约不一致（接管场景）时，
   游标不可信，回退到有效提交点；回退窗口内的到期项一律按可能已投递处理。

## 恢复顺序与输出

`recover()` 返回 `RecoveryPlan`：

- `recovery_order`：先 `DELETE` 清理，再 `REBUILD`（按 not-before 排序），最后 `WAIT`。
- `duplicate_risk_ids`：重建时可能重复投递的消息 ID，重投必须走幂等键。
- `remaining_delayed`：删除项之外、仍需保留的延迟集合，按 not-before、position 排序。
- `safe_cursor` / `effective_commit_point`：游标与提交点钳制后的安全值。
- `risk_notes`：游标回退、提交点越界、offset 不一致等风险说明。

运行演示：

```bash
python -m delayed_index.demo
```

## 测试

```bash
python -m unittest
```

覆盖场景（`tests/test_recovery.py`）：

- 索引崩溃后 DELETE / REBUILD / WAIT 混合恢复。
- 重复投递：模拟队列幂等重投，确认后残留项转为 DELETE；offset 不一致带风险重建。
- 游标回退：租约过期与新租约接管都不能推进旧游标。
- 租约接管：fencing token 不匹配时游标回到提交点。
- 提交点不得越过未确认的投递缺口。
