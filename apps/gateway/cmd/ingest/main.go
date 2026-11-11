// Command ingest 知识库文档入库 CLI
//
// 用法：
//
//	ingest -config configs/config.yaml -project ops -path ./docs
//	ingest -config configs/config.yaml -project ops -path ./docs/alert.md -title "告警处理手册" -type md
//
// 行为：
//   - path 为目录时递归处理 .md / .txt；为单文件时只处理该文件
//   - doc_key 默认取相对 path 的路径（同路径重复入库幂等：checksum 未变直接跳过）
//   - 入库成功按 chunk 批量 embedding 写入 Qdrant collection kb_<slug>，
//     并更新 documents / projects.index_epoch（语义缓存旧答案 60s 内失效）
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"ai-gateway/apps/gateway/internal/rag"
	"ai-gateway/pkg/config"
	"ai-gateway/pkg/db"
	"ai-gateway/pkg/observability"
	"ai-gateway/pkg/vector"
	"go.uber.org/zap"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "ingest failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", config.DefaultPath, "配置文件路径")
	project := flag.String("project", "", "项目 slug（projects.slug）")
	path := flag.String("path", "", "文件或目录路径")
	title := flag.String("title", "", "文档标题（单文件时使用；目录时默认取文件名）")
	sourceType := flag.String("type", "", "源类型：md / txt（默认按扩展名推断）")
	docKey := flag.String("doc-key", "", "文档唯一键（默认相对路径）")
	tenantID := flag.Int64("tenant", 0, "租户 ID（>0 时校验项目归属）")
	flag.Parse()

	if *project == "" || *path == "" {
		return fmt.Errorf("-project 与 -path 必填")
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if cfg.Qdrant.Addr == "" || cfg.Qdrant.EmbeddingURL == "" {
		return fmt.Errorf("qdrant.addr / qdrant.embedding_url 未配置，无法入库")
	}
	if err := observability.Init("info"); err != nil {
		return err
	}
	defer observability.Sync()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	pool, err := db.InitPool(ctx, cfg.MySQL.DSN, cfg.MySQL.PoolSize)
	if err != nil {
		return err
	}
	defer pool.Close()

	svc := rag.NewService(
		pool,
		vector.NewEmbeddingClient(
			cfg.Qdrant.EmbeddingURL,
			cfg.Qdrant.EmbeddingModel,
			os.Getenv(cfg.Qdrant.EmbeddingAPIKeyEnv),
		),
		vector.NewQdrantClient(cfg.Qdrant.Addr),
		rag.Options{
			CollectionPrefix: cfg.RAG.CollectionPrefix,
			TopK:             cfg.RAG.TopK,
			ScoreThreshold:   cfg.RAG.ScoreThreshold,
			MaxContextTokens: cfg.RAG.MaxContextTokens,
			RetrievalTimeout: cfg.RAG.RetrievalTimeoutDuration(),
			ChunkSize:        cfg.RAG.ChunkSize,
			ChunkOverlap:     cfg.RAG.ChunkOverlap,
			EmbedModel:       cfg.Qdrant.EmbeddingModel,
		},
	)
	defer svc.Close()

	files, err := collectFiles(*path)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("%s 下没有可入库的 .md / .txt 文件", *path)
	}

	var total, skipped, chunks int
	for _, file := range files {
		content, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("读取 %s 失败: %w", file, err)
		}
		key := *docKey
		if key == "" || len(files) > 1 {
			rel, rerr := filepath.Rel(*path, file)
			if rerr != nil || rel == "." {
				rel = filepath.Base(file)
			}
			key = rel
		}
		docTitle := *title
		if docTitle == "" || len(files) > 1 {
			docTitle = strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))
		}
		st := *sourceType
		if st == "" {
			st = strings.TrimPrefix(strings.ToLower(filepath.Ext(file)), ".")
		}

		res, err := svc.Ingest(ctx, rag.IngestRequest{
			TenantID:    *tenantID,
			ProjectSlug: *project,
			DocKey:      key,
			Title:       docTitle,
			SourceType:  st,
			Content:     string(content),
		})
		if err != nil {
			return fmt.Errorf("入库 %s 失败: %w", key, err)
		}
		total++
		if res.Skipped {
			skipped++
			fmt.Printf("SKIP %s（内容未变化）\n", key)
			continue
		}
		chunks += res.ChunkCount
		fmt.Printf("OK   %s -> doc_id=%d chunks=%d\n", key, res.DocID, res.ChunkCount)
	}
	observability.L().Info("ingest finished",
		zap.String("project", *project),
		zap.Int("documents", total),
		zap.Int("skipped", skipped),
		zap.Int("chunks", chunks),
	)
	return nil
}

// collectFiles 收集 .md / .txt：单文件直接返回，目录递归
func collectFiles(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []string{path}, nil
	}
	var files []string
	err = filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		switch strings.ToLower(filepath.Ext(p)) {
		case ".md", ".txt":
			files = append(files, p)
		}
		return nil
	})
	return files, err
}
