// Package dbtest 内存版 database/sql 驱动 fake：替代真实 MySQL 的单测辅助
//
// 用法：
//
//	db, fake := dbtest.New(t)
//	fake.ExpectQuery("FROM api_keys").WillReturnRows(
//		[]string{"id", "tenant_id"},
//		[]driver.Value{int64(2), int64(1)},
//	)
//	// ... 被测代码通过 dbPool 钩子拿到 db ...
//	fake.AssertExpectations(t)
//
// 匹配语义（与 sqlmock 有序模式一致）：
//   - 期望按注册顺序消费，每条期望只匹配一次（contains 子串匹配 SQL）
//   - 未匹配到期望的查询立即返回错误（用于精确断言"缓存命中后零 DB 查询"）
//   - AssertExpectations 校验所有期望都被消费
package dbtest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func init() {
	sql.Register("dbtest", &stubDriver{})
}

var (
	seq      atomic.Int64
	registry sync.Map // dsn name -> *FakeDB
)

// New 创建一个 fake *sql.DB 与其控制器；测试结束自动关闭
func New(t *testing.T) (*sql.DB, *FakeDB) {
	t.Helper()
	name := fmt.Sprintf("dbtest-%d", seq.Add(1))
	fake := &FakeDB{}
	registry.Store(name, fake)

	db, err := sql.Open("dbtest", name)
	if err != nil {
		t.Fatalf("dbtest: open failed: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
		registry.Delete(name)
	})
	return db, fake
}

// Call 一次真实的 SQL 调用记录
type Call struct {
	Method string // "query" / "exec"
	SQL    string
	Args   []driver.NamedValue
}

// Expectation 一条预设期望
type Expectation struct {
	kind     string
	contains string
	columns  []string
	rows     [][]driver.Value
	result   driver.Result
	err      error
	used     bool
}

// WillReturnRows 设置查询返回的行列
func (e *Expectation) WillReturnRows(columns []string, rows ...[]driver.Value) *Expectation {
	e.columns = columns
	e.rows = rows
	return e
}

// WillReturnError 设置查询/执行返回错误
func (e *Expectation) WillReturnError(err error) *Expectation {
	e.err = err
	return e
}

// WillReturnResult 设置 Exec 的 LastInsertId / RowsAffected
func (e *Expectation) WillReturnResult(lastInsertID, rowsAffected int64) *Expectation {
	e.result = stubResult{lastID: lastInsertID, affected: rowsAffected}
	return e
}

// FakeDB 期望与调用记录的控制器
type FakeDB struct {
	mu      sync.Mutex
	expects []*Expectation
	calls   []Call
}

// ExpectQuery 注册一条查询期望（contains 为 SQL 子串）
func (f *FakeDB) ExpectQuery(contains string) *Expectation {
	e := &Expectation{kind: "query", contains: contains}
	f.mu.Lock()
	f.expects = append(f.expects, e)
	f.mu.Unlock()
	return e
}

// ExpectExec 注册一条执行期望
func (f *FakeDB) ExpectExec(contains string) *Expectation {
	e := &Expectation{kind: "exec", contains: contains}
	f.mu.Lock()
	f.expects = append(f.expects, e)
	f.mu.Unlock()
	return e
}

// Calls 返回所有已发生的调用
func (f *FakeDB) Calls() []Call {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Call, len(f.calls))
	copy(out, f.calls)
	return out
}

// AssertExpectations 校验所有期望都被恰好消费一次
func (f *FakeDB) AssertExpectations(t *testing.T) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, e := range f.expects {
		if !e.used {
			t.Errorf("期望 #%d（%s %q）未被消费", i, e.kind, e.contains)
		}
	}
}

// match 找到第一条未消费且类型、子串都匹配的期望
func (f *FakeDB) match(kind, query string) (*Expectation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.expects {
		if e.used || e.kind != kind {
			continue
		}
		if strings.Contains(query, e.contains) {
			e.used = true
			return e, nil
		}
	}
	return nil, fmt.Errorf("dbtest: unexpected %s: %s", kind, query)
}

func (f *FakeDB) record(c Call) {
	f.mu.Lock()
	f.calls = append(f.calls, c)
	f.mu.Unlock()
}

// --- driver 实现 ---

type stubDriver struct{}

func (d *stubDriver) Open(name string) (driver.Conn, error) {
	v, ok := registry.Load(name)
	if !ok {
		return nil, fmt.Errorf("dbtest: unknown dsn %q", name)
	}
	return &conn{fake: v.(*FakeDB)}, nil
}

type conn struct {
	fake *FakeDB
}

func (c *conn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("dbtest: prepare not supported")
}

func (c *conn) Close() error { return nil }

func (c *conn) Begin() (driver.Tx, error) {
	return nil, errors.New("dbtest: transaction not supported")
}

// CheckNamedValue 接受全部参数类型（跳过默认转换限制）
func (c *conn) CheckNamedValue(*driver.NamedValue) error { return nil }

func (c *conn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.fake.record(Call{Method: "query", SQL: query, Args: args})
	e, err := c.fake.match("query", query)
	if err != nil {
		return nil, err
	}
	if e.err != nil {
		return nil, e.err
	}
	return &stubRows{columns: e.columns, data: e.rows}, nil
}

func (c *conn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.fake.record(Call{Method: "exec", SQL: query, Args: args})
	e, err := c.fake.match("exec", query)
	if err != nil {
		return nil, err
	}
	if e.err != nil {
		return nil, e.err
	}
	if e.result == nil {
		return stubResult{}, nil
	}
	return e.result, nil
}

type stubRows struct {
	columns []string
	data    [][]driver.Value
	pos     int
}

func (r *stubRows) Columns() []string { return r.columns }
func (r *stubRows) Close() error      { return nil }

func (r *stubRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.pos])
	r.pos++
	return nil
}

type stubResult struct {
	lastID   int64
	affected int64
}

func (r stubResult) LastInsertId() (int64, error) { return r.lastID, nil }
func (r stubResult) RowsAffected() (int64, error) { return r.affected, nil }
