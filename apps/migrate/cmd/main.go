// Migrate CLI — 数据库迁移工具
//
// 命令：
//
//	go run . up [N]         执行所有未执行的迁移（或 N 个）
//	go run . down [N]       回滚最后一个迁移（或 N 个）
//	go run . create <name>  创建新的 up/down SQL 迁移文件
//	go run . version        显示当前数据库迁移版本
//	go run . force <N>      强制设置版本号（修复脏状态：dirty → clean）
//
// 底层：golang-migrate v4
// SQL 文件：项目根 sql/ 目录
// DSN 来源：config.yaml 的 mysql.dsn 字段（默认 apps/gateway/configs/config.yaml）
//
// 示例：
//
//	cd apps/migrate && go run . up           # 跑所有迁移
//	cd apps/migrate && go run . up 1         # 只跑 1 个迁移
//	cd apps/migrate && go run . down         # 回滚最后一个
//	cd apps/migrate && go run . create       # 创建 003_xxx.up.sql + 003_xxx.down.sql
//	cd apps/migrate && go run . force 2      # 强制版本设为 2（001 + 002 视为已执行）
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"ai-gateway/pkg/config"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const (
	// 默认配置文件路径（相对 apps/migrate/cmd/）
	defaultConfigPath = "../../gateway/configs/config.yaml"
	// SQL 迁移文件目录（相对 apps/migrate/cmd/）
	defaultSQLDir = "../../../sql"
)

func main() {
	// 手动解析 --config flag，因为 Go flag 库对 os.Args[1:] 处理会吞掉命令
	configPath := defaultConfigPath
	var args []string

	for i := 1; i < len(os.Args); i++ {
		a := os.Args[i]
		if a == "--config" || a == "-config" {
			if i+1 < len(os.Args) {
				configPath = os.Args[i+1]
				i++
			}
			continue
		}
		if strings.HasPrefix(a, "--config=") {
			configPath = strings.TrimPrefix(a, "--config=")
			continue
		}
		if strings.HasPrefix(a, "-config=") {
			configPath = strings.TrimPrefix(a, "-config=")
			continue
		}
		args = append(args, a)
	}

	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	cmd := args[0]
	rest := args[1:]

	switch cmd {
	case "up":
		os.Exit(runUp(configPath, rest))
	case "down":
		os.Exit(runDown(configPath, rest))
	case "create":
		os.Exit(runCreate(rest))
	case "version":
		os.Exit(runVersion(configPath))
	case "force":
		os.Exit(runForce(configPath, rest))
	case "--help", "-h", "help":
		printUsage()
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "未知命令: %s\n\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Migrate CLI — 数据库迁移工具

用法：
  migrate up [N]           执行所有未执行的迁移（或 N 个）
  migrate down [N]         回滚最后一个迁移（或 N 个）
  migrate create <name>    创建新的 up/down SQL 迁移文件
  migrate version          显示当前数据库迁移版本
  migrate force <N>        强制设置版本号（修复脏状态）

参数：
  --config <path>          配置文件路径（默认 gateway/configs/config.yaml）

SQL 文件目录：项目根 sql/
底层引擎：golang-migrate v4 + MySQL`)
}

// --- 通用：定位 sql 目录 + 创建 migrate 实例 ---

// resolveSQLDir 从项目根定位 sql/ 目录
// 策略：先尝试 defaultSQLDir（cmd/ 相对路径），失败则尝试 go.mod 同级
func resolveSQLDir() (string, error) {
	// 1. 尝试 defaultSQLDir（相对于 apps/migrate/cmd/）
	if abs, err := filepath.Abs(defaultSQLDir); err == nil {
		if info, err := os.Stat(abs); err == nil && info.IsDir() {
			return abs, nil
		}
	}

	// 2. 尝试当前工作目录 + /sql
	cwd, _ := os.Getwd()
	candidate := filepath.Join(cwd, "sql")
	if info, err := os.Stat(candidate); err == nil && info.IsDir() {
		return candidate, nil
	}

	// 3. 向上两级找项目根 sql/（apps/migrate/cmd → apps/migrate → 项目根 → sql）
	for i := 0; i < 5; i++ {
		if info, err := os.Stat(filepath.Join(cwd, "sql")); err == nil && info.IsDir() {
			return filepath.Join(cwd, "sql"), nil
		}
		parent := filepath.Dir(cwd)
		if parent == cwd {
			break
		}
		cwd = parent
	}

	return "", fmt.Errorf("找不到 sql/ 目录，请在项目根或 apps/migrate 下运行")
}

func newMigrate(configPath string) (*migrate.Migrate, string, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, "", fmt.Errorf("加载配置失败: %w", err)
	}

	sqlDir, err := resolveSQLDir()
	if err != nil {
		return nil, "", err
	}

	sourceURL := "file://" + sqlDir
	dbURL := mysqlDSNtoMigrateDSN(cfg.MySQL.DSN)

	m, err := migrate.New(sourceURL, dbURL)
	if err != nil {
		return nil, "", fmt.Errorf("创建 migrate 实例失败: %w", err)
	}

	return m, sqlDir, nil
}

// mysqlDSNtoMigrateDSN 把标准 MySQL DSN 转成 golang-migrate 能识别的 URL 格式
// 输入：root:root@tcp(localhost:3306)/ai_gateway?charset=utf8mb4...
// 输出：mysql://root:root@tcp(localhost:3306)/ai_gateway?charset=utf8mb4...
func mysqlDSNtoMigrateDSN(dsn string) string {
	if strings.HasPrefix(dsn, "mysql://") {
		return dsn
	}
	return "mysql://" + dsn
}

// --- up ---

func runUp(configPath string, args []string) int {
	m, _, err := newMigrate(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer m.Close()

	if len(args) > 0 {
		n, err := strconv.Atoi(args[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, "参数错误: 期望数字，得到 %q\n", args[0])
			return 1
		}
		if err := m.Steps(n); err != nil {
			if err == migrate.ErrNoChange {
				fmt.Println("✅ 已是最新版本，无需迁移")
				return 0
			}
			fmt.Fprintf(os.Stderr, "❌ 迁移失败: %v\n", err)
			return 1
		}
		fmt.Printf("✅ 成功执行 %d 个迁移\n", n)
	} else {
		if err := m.Up(); err != nil {
			if err == migrate.ErrNoChange {
				fmt.Println("✅ 已是最新版本，无需迁移")
				return 0
			}
			fmt.Fprintf(os.Stderr, "❌ 迁移失败: %v\n", err)
			return 1
		}
		fmt.Println("✅ 所有迁移执行完毕")
	}

	printVersion(m)
	return 0
}

// --- down ---

func runDown(configPath string, args []string) int {
	m, _, err := newMigrate(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer m.Close()

	n := 1
	if len(args) > 0 {
		var err error
		n, err = strconv.Atoi(args[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, "参数错误: 期望数字，得到 %q\n", args[0])
			return 1
		}
	}

	if err := m.Steps(-n); err != nil {
		if err == migrate.ErrNoChange {
			fmt.Println("✅ 已是最初版本，无需回滚")
			return 0
		}
		fmt.Fprintf(os.Stderr, "❌ 回滚失败: %v\n", err)
		return 1
	}
	fmt.Printf("✅ 成功回滚 %d 个迁移\n", n)
	printVersion(m)
	return 0
}

// --- create ---

func runCreate(args []string) int {
	// 从项目根定位 sql/ 目录：apps/migrate/cmd → 上两级 → 项目根/sql
	sqlDir, err := resolveSQLDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ 定位 sql/ 目录失败: %v\n", err)
		return 1
	}

	name := "new_migration"
	if len(args) > 0 {
		name = strings.ReplaceAll(args[0], " ", "_")
	}

	// 1. 先查现有最大版本号
	maxVersion := getMaxVersion(sqlDir)

	// 2. 新版本号 = max + 1，三位补零
	newVersion := maxVersion + 1
	versionStr := fmt.Sprintf("%03d", newVersion)

	// 3. 构造文件名
	baseName := fmt.Sprintf("%s_%s", versionStr, name)
	upFile := filepath.Join(sqlDir, baseName+".up.sql")
	downFile := filepath.Join(sqlDir, baseName+".down.sql")

	// 4. 检查文件是否已存在
	if fileExists(upFile) || fileExists(downFile) {
		fmt.Fprintf(os.Stderr, "文件已存在: %s 或 %s\n", upFile, downFile)
		return 1
	}

	// 5. 创建空文件
	timestamp := time.Now().Format("2006-01-02")
	upContent := fmt.Sprintf("-- Migration: %s\n-- Created: %s\n\n", baseName, timestamp)
	downContent := fmt.Sprintf("-- Rollback for %s\n\n", baseName)

	if err := os.WriteFile(upFile, []byte(upContent), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "创建 %s 失败: %v\n", upFile, err)
		return 1
	}
	if err := os.WriteFile(downFile, []byte(downContent), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "创建 %s 失败: %v\n", downFile, err)
		os.Remove(upFile) // 回滚已创建的 up 文件
		return 1
	}

	fmt.Printf("✅ 创建迁移文件:\n  %s\n  %s\n", upFile, downFile)
	return 0
}

// getMaxVersion 扫描 sqlDir 目录，返回现有最大版本号
func getMaxVersion(sqlDir string) int {
	entries, err := os.ReadDir(sqlDir)
	if err != nil {
		return 0
	}
	max := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		// 匹配 pattern: NNN_*.up.sql / NNN_*.down.sql
		if idx := strings.Index(name, "_"); idx >= 3 {
			verStr := name[:idx]
			if v, err := strconv.Atoi(verStr); err == nil && v > max {
				max = v
			}
		}
	}
	return max
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// --- version / force ---

func runVersion(configPath string) int {
	m, _, err := newMigrate(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer m.Close()

	printVersion(m)
	return 0
}

func printVersion(m *migrate.Migrate) {
	version, dirty, err := m.Version()
	if err != nil {
		if err == migrate.ErrNilVersion {
			fmt.Println("📦 数据库从未执行过迁移（version = 0）")
			return
		}
		fmt.Fprintf(os.Stderr, "❌ 获取版本失败: %v\n", err)
		return
	}
	dirtyTag := ""
	if dirty {
		dirtyTag = " ⚠️  DIRTY（上次迁移中途失败，需要 force 修复）"
	}
	fmt.Printf("📦 当前迁移版本: %d%s\n", version, dirtyTag)
}

func runForce(configPath string, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "请指定版本号，例如: migrate force 0")
		return 1
	}

	version, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "版本号错误: %q\n", args[0])
		return 1
	}

	m, _, err := newMigrate(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer m.Close()

	if err := m.Force(version); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Force 失败: %v\n", err)
		return 1
	}
	fmt.Printf("✅ 强制版本设为: %d\n", version)
	printVersion(m)
	return 0
}
