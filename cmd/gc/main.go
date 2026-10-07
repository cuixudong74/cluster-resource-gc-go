// gc 命令行：从本地快照识别遗留资源，先输出干跑计划，确认后执行。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	gc "github.com/cuixudong74/cluster-resource-gc-go/pkg/gc"
)

func main() {
	var (
		snapshotPath  = flag.String("snapshot", "snapshot.json", "本地快照文件路径（JSON）")
		configPath    = flag.String("config", "", "回收配置文件路径（JSON，可选）")
		dryRun        = flag.Bool("dry-run", true, "只输出回收计划，不执行删除")
		checkpointDir = flag.String("checkpoint-dir", ".gc-checkpoints", "检查点目录")
		actor         = flag.String("actor", "cli", "操作者标识（用于授权与审计）")
		batchSize     = flag.Int("batch-size", 50, "每批回收对象数")
		concurrency   = flag.Int("concurrency", 4, "单批内并发删除数")
		rate          = flag.Float64("rate", 0, "每秒最大删除数，0 表示不限速")
		timeout       = flag.Duration("timeout", 10*time.Minute, "整体执行超时")
	)
	flag.Parse()

	cfg := gc.Config{}
	if *configPath != "" {
		data, err := os.ReadFile(*configPath)
		if err != nil {
			fatal("read config: %v", err)
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			fatal("parse config: %v", err)
		}
	}
	cfg.Plan.BatchSize = *batchSize
	cfg.Executor.Concurrency = *concurrency
	cfg.Executor.RatePerSecond = *rate

	// 本地运行：删除落点为“从快照副本中移除”，不触碰真实集群。
	snap, err := (&gc.FileSource{Path: *snapshotPath}).Load(context.Background())
	if err != nil {
		fatal("load snapshot: %v", err)
	}
	existing := make([]gc.ObjectID, 0, len(snap.Resources))
	for _, r := range snap.Resources {
		existing = append(existing, r.ID)
	}
	deleter := gc.NewMemoryDeleter(existing)

	g := &gc.GC{
		Source:  &gc.FileSource{Path: *snapshotPath},
		Deleter: deleter,
		Store:   &gc.FileCheckpointStore{Dir: *checkpointDir},
		Auditor: &gc.MemoryAuditor{},
		Auth:    &gc.StaticAuthorizer{Allowed: map[string]bool{"*": true}},
		Config:  cfg,
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	ctx = gc.WithActor(ctx, *actor)

	plan, err := g.DryRun(ctx)
	if err != nil {
		fatal("dry-run: %v", err)
	}
	out, _ := json.MarshalIndent(plan, "", "  ")
	fmt.Println("=== 干跑回收计划 ===")
	fmt.Println(string(out))

	if *dryRun {
		fmt.Println("dry-run 模式，未执行任何删除。")
		return
	}

	exec := &gc.Executor{
		Deleter: g.Deleter, Store: g.Store, Auditor: g.Auditor, Auth: g.Auth,
		Opts: cfg.Executor,
	}
	res, err := exec.Execute(ctx, plan)
	if err != nil {
		fmt.Fprintf(os.Stderr, "执行中断: %v（已提交进度可从检查点恢复）\n", err)
	}
	resOut, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println("=== 执行结果 ===")
	fmt.Println(string(resOut))
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
