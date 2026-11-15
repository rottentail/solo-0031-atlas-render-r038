# 运行与恢复

## 进程拓扑

服务拆分为两类进程，共享同一个状态文件：

- `atlas serve` 只负责接收、查询和控制。它不执行渲染，也不持有任务。
- `atlas worker` 是执行进程，可以同时运行任意多个。每个进程领取任务、续租并提交结果。

两类进程都通过 `ATLAS_STATE` 指向同一个状态路径。区别只是角色：HTTP 入口不再内嵌执行器。至少需要 1 个 `serve` 和 1 个 `worker` 才能完成渲染；`serve` 可以单独运行，此时任务保持 `queued`。

```sh
go build -o atlas ./cmd/atlas
ATLAS_ADDR=127.0.0.1:8763 ATLAS_STATE=/var/lib/atlas/state.json ./atlas serve
ATLAS_STATE=/var/lib/atlas/state.json ATLAS_WORKER_ID=worker-a ./atlas worker
ATLAS_STATE=/var/lib/atlas/state.json ATLAS_WORKER_ID=worker-b ./atlas worker
```

## 从单进程迁移

旧版本以单个进程同时承担 HTTP 和渲染。新格式只增加可选的 `claim`、`workerId`、`leaseUntil` 字段和事件的 `actor`，旧文件可直接读取：

1. 正常停止旧进程（SIGTERM）。它会等待请求结束；自行中断的任务保持 `running`。
2. 部署新二进制，启动 `atlas serve`。启动校验通过后，HTTP 立即可用。
3. 启动一个或多个 `atlas worker`。启动时以及每次领取事务中，任何租约已过期的 `running` 任务会回到 `queued`，attempt 不变。
4. 观察 `GET /v1/status`：`activeWorkers` 列出正在执行的 worker，`jobs.running` 应逐步归零。

迁移不需要改写状态，也不需要停机窗口之外的数据修复。旧格式的 `running` 记录没有租约字段，会被当作过期任务重新入队（见下）。

## 租约与失联

领取任务时写入 `claim`（随机执行令牌）、`workerId` 和 `leaseUntil`。执行进程按 `ATLAS_LEASE/4` 轮询续租：

- 续租只在自己仍持有相同 `claim` 和 `attempt` 时生效。
- 提交结果使用同一组条件。取消、重试或另一进程接管都会清除旧 `claim`，因此旧执行进程即使完成渲染也无法发布结果。
- 租约过期后，任意进程（执行进程领取时的重扫，或 `serve` 中的重扫循环）会把任务退回 `queued`，attempt 不变，并记录 `job.recovered` 事件。
- 优雅停止（SIGTERM）会把自己的未完成任务立即退回 `queued` 并记录 `job.released`，无需等待租约到期。

`serve` 每次轮询只在观察到 `queued` 任务或过期租约时才写事务，空闲时只读。

`ATLAS_LEASE` 默认 5s。它必须明显大于一次续租往返的存储延迟；跨机器网络文件系统未验证，不要依赖本机制实现跨主机高可用。

## 取消、重试与尝试隔离

取消在持久事务中把状态改为 `cancelled` 并清除 `claim`。执行进程在下一次心跳发现 `claim` 不再匹配，随即中断本地渲染。若渲染先完成而取消先提交，结果提交事务因 `claim` 缺失而放弃，不会出现“取消后又成功”。

重试把 `failed`/`cancelled` 改回 `queued`、attempt 加一并清除 `claim`。旧 attempt 的执行进程即使仍在运行也无法提交。

## 调度与容量

多个 worker 竞争同一把文件锁；领取事务保证一个任务在同一时刻只有一个所有者。按 `createdAt`、`id` 排序领取最旧任务。单个进程可用 `ATLAS_WORKERS=N` 提高并发，但仍受队列上限 64 和整体状态提交成本的约束。

文件锁在事务期间持有。读取最多等待 2s、写入最多 5s，超时返回 500 而不是无限阻塞；心跳与空闲轮询的只读探测最多等待 750ms，超时按“稍后重试”处理。进程崩溃会关闭文件描述符并释放锁，因此 `SIGKILL` 或断电不会留下持锁进程。唯一例外是被 `SIGSTOP` 冻结在事务内部的进程：它仍然持有锁，直到被恢复或杀掉；这是挂起而非失败，不通过关闭锁规避，需要运维恢复或终止该进程。

`GET /v1/status` 的 `activeWorkers` 显示每个 `workerId` 正在执行的任务数；`jobs` 是各状态计数；`artifactBytes` 是持久结果字节总量。历史事件超过 500 条会截断，但 `sequence` 继续增加。

## 持久性与故障

每个事务在锁内重新从磁盘读取状态，校验候选状态，写临时文件并 fsync，再原子替换。进程内存不再作为事实来源；任何进程崩溃都不会留下半个事务。写失败时磁盘上的旧状态保持可读。

故障时保留状态文件和服务日志，在副本上检查。500 表示事务未完成；客户端可读取原请求目标确认状态。提交任务时使用稳定幂等键，不能因为连接中断盲目换键。409 应结合场景 revision、快照 state 或收据语义判断。410 说明成功结果字节已清理，任务成功事实仍保留。

备份时先正常停止 `serve` 与全部 worker，复制状态 JSON；恢复时向空的独立状态路径复制，再启动。启动校验拒绝不支持版本、损坏摘要、孤立关联和非法文档。备份包含 PNG 字节与全部快照引用闭包。

## 真实失联验证

`python3 scripts/walkthrough.py --workflow partition-recovery` 演练执行进程在执行中崩溃：单 worker 领取重负载任务后被 `SIGKILL`，内核释放其文件锁；API 保持在线并显示该 attempt 仍为 `running`；租约过期后替代 worker 接管同一 attempt（不增加次数）并成功，脚本断言最终只有一个 artifact、attempt 仍为 1。

“进程仍存活但 `claim` 已被取消或接管”的竞态由 `go test ./internal/engine` 中的 `TestFinishRejectsCancelledClaim`、`TestFinishRejectsStaleAttempt` 确定性地验证，不依赖信号落点。`TestFinishPublishesOwnedClaim` 验证持有者正常发布并清除租约。

`python3 scripts/walkthrough.py --workflow recover-render` 演练杀掉执行进程：API 保持在线，新 worker 在租约过期后接管并完成原 attempt。

`python3 scripts/walkthrough.py --workflow multi-worker` 演练三进程并发消费、运行中取消不产生结果、优雅停止回队和旧格式 `running` 记录继续处理。

从旧单进程部署迁移后的兼容性可额外自证：用旧二进制在渲染中断时杀死进程，得到没有 `claim`/`leaseUntil` 的 `running` 记录；再用新的 `atlas serve` 加 `atlas worker` 指向同一状态文件，任务会被当作过期 attempt 重新领取并以 attempt 1 完成。

## systemd

`deploy/atlas.service` 运行 API；`deploy/atlas-worker@.service` 是执行进程模板。按实际用户、二进制和状态路径调整：

```sh
systemctl enable --now atlas.service
systemctl enable --now atlas-worker@1.service atlas-worker@2.service
```

多个执行进程共享状态是预期用法；第二个 API 进程需要不同的 `ATLAS_ADDR`，否则会因端口冲突无法监听。锁文件可长期存在，锁的有效性由操作系统进程锁决定。不要通过删除锁文件、关闭锁或引入共享内存状态来绕过事务。

当前未验证容器部署、跨机器文件系统锁或网络存储。macOS 可直接运行同一二进制。
