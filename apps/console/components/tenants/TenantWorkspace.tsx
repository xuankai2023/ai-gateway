"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import {
  Alert,
  App,
  Button,
  Input,
  Popconfirm,
  Select,
  Space,
  Table,
} from "antd";
import type { ColumnsType } from "antd/es/table";
import KeysDrawer from "@/components/tenants/KeysDrawer";
import TenantEditor from "@/components/tenants/TenantEditor";
import { listTenants, updateTenant } from "@/lib/api/client";
import type { Tenant } from "@/lib/types";

const PAGE_SIZE = 20;

// 状态点徽标：启用为品牌绿，停用为灰点
const BADGE = "inline-flex items-center gap-1.5 text-xs before:h-1.5 before:w-1.5 before:rounded-full";
const BADGE_ON = `${BADGE} before:bg-brand`;
const BADGE_OFF = `${BADGE} before:bg-[#b9bdb9]`;

function formatTime(value: string | null) {
  if (!value) return "—";
  return new Date(value).toLocaleString("zh-CN", { hour12: false });
}

function limitText(tenant: Tenant) {
  const parts: string[] = [];
  if (tenant.rps_limit) parts.push(`${tenant.rps_limit} rps`);
  if (tenant.tpm_limit) parts.push(`${tenant.tpm_limit} tpm`);
  if (tenant.concurrency_limit) parts.push(`${tenant.concurrency_limit} 并发`);
  return parts.length > 0 ? parts.join(" · ") : "默认";
}

export default function TenantWorkspace() {
  const { message } = App.useApp();

  const [rows, setRows] = useState<Tenant[]>([]);
  const [nextCursor, setNextCursor] = useState<number | null>(null);
  const [starts, setStarts] = useState<(number | null)[]>([null]);
  const [pageIndex, setPageIndex] = useState(0);
  const [q, setQ] = useState("");
  const [status, setStatus] = useState("");
  const [refreshKey, setRefreshKey] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [busyId, setBusyId] = useState<number | null>(null);

  const [editorOpen, setEditorOpen] = useState(false);
  const [editing, setEditing] = useState<Tenant | null>(null);
  const [keysTenant, setKeysTenant] = useState<Tenant | null>(null);

  const requestId = useRef(0);

  const load = useCallback(
    async (cursor: number | null) => {
      const current = ++requestId.current;
      setLoading(true);
      setError("");
      try {
        const page = await listTenants({
          cursor,
          limit: PAGE_SIZE,
          q,
          status,
        });
        if (current !== requestId.current) return;
        setRows(page.data ?? []);
        setNextCursor(page.next_cursor ?? null);
      } catch (err) {
        if (current !== requestId.current) return;
        setError(err instanceof Error ? err.message : "租户列表加载失败");
      } finally {
        if (current === requestId.current) setLoading(false);
      }
    },
    [q, status],
  );

  useEffect(() => {
    void load(starts[pageIndex] ?? null);
  }, [load, starts, pageIndex, refreshKey]);

  function applyFilters(nextQ: string, nextStatus: string) {
    setQ(nextQ);
    setStatus(nextStatus);
    setStarts([null]);
    setPageIndex(0);
    setRefreshKey((k) => k + 1);
  }

  function reload() {
    setRefreshKey((k) => k + 1);
  }

  function goNext() {
    if (nextCursor == null) return;
    setStarts((prev) => [...prev.slice(0, pageIndex + 1), nextCursor]);
    setPageIndex((i) => i + 1);
  }

  function goPrev() {
    setPageIndex((i) => Math.max(0, i - 1));
  }

  async function toggleStatus(tenant: Tenant) {
    const next = tenant.status === "active" ? "disabled" : "active";
    setBusyId(tenant.id);
    try {
      await updateTenant(tenant.id, { status: next });
      message.success(next === "active" ? "租户已启用" : "租户已停用");
      reload();
    } catch (err) {
      message.error(err instanceof Error ? err.message : "操作失败");
    } finally {
      setBusyId(null);
    }
  }

  const columns: ColumnsType<Tenant> = [
    {
      title: "租户",
      dataIndex: "name",
      width: 190,
      render: (_, row) => (
        <div>
          <div className="font-medium">{row.name}</div>
          <div className="mt-0.5 font-mono text-xs text-muted">
            #{row.id} · {row.tier}
          </div>
        </div>
      ),
    },
    {
      title: "状态",
      dataIndex: "status",
      width: 80,
      render: (value: string) => (
        <span className={value === "active" ? BADGE_ON : BADGE_OFF}>
          {value === "active" ? "启用" : "停用"}
        </span>
      ),
    },
    {
      title: "月度预算",
      dataIndex: "budget_monthly",
      width: 110,
      render: (value: number) =>
        value > 0 ? (
          <span className="font-mono text-xs">${value.toFixed(2)}</span>
        ) : (
          <span className="text-xs text-muted">不限额</span>
        ),
    },
    {
      title: "限流",
      key: "limits",
      width: 160,
      render: (_, row) => <span className="font-mono text-xs">{limitText(row)}</span>,
    },
    {
      title: "语义缓存",
      dataIndex: "cache_enabled",
      width: 80,
      responsive: ["xl"],
      render: (value: boolean) => (
        <span className={value ? BADGE_ON : BADGE_OFF}>{value ? "开" : "关"}</span>
      ),
    },
    {
      title: "创建时间",
      dataIndex: "created_at",
      width: 160,
      responsive: ["xl"],
      render: (value: string | null) => (
        <span className="font-mono text-xs">{formatTime(value)}</span>
      ),
    },
    {
      title: "操作",
      key: "actions",
      width: 170,
      render: (_, row) => (
        <Space size={0}>
          <Button type="link" size="small" onClick={() => setKeysTenant(row)}>
            Keys
          </Button>
          <Button
            type="link"
            size="small"
            onClick={() => {
              setEditing(row);
              setEditorOpen(true);
            }}
          >
            编辑
          </Button>
          {row.status === "active" ? (
            <Popconfirm
              title="停用租户"
              description="停用后该租户的所有请求会被拒绝。"
              okText="停用"
              cancelText="取消"
              okButtonProps={{ danger: true }}
              onConfirm={() => toggleStatus(row)}
            >
              <Button type="link" size="small" danger loading={busyId === row.id}>
                停用
              </Button>
            </Popconfirm>
          ) : (
            <Button
              type="link"
              size="small"
              loading={busyId === row.id}
              onClick={() => toggleStatus(row)}
            >
              启用
            </Button>
          )}
        </Space>
      ),
    },
  ];

  return (
    <div className="pt-[30px]">
      <div className="mb-[22px] flex flex-col items-start justify-between gap-6 min-[900px]:flex-row min-[900px]:items-end">
        <div>
          <div className="mb-2 text-[11px] uppercase tracking-[0.2em] text-muted">Tenants</div>
          <h1 className="m-0 text-3xl font-semibold leading-[1.15] tracking-[-0.02em]">租户管理</h1>
          <p className="mt-2 max-w-[620px] text-[13px] leading-[1.7] text-muted">
            租户是配额、账本和 API Key 的归属单位；启停、预算与限流变更会即时广播到所有网关节点。
          </p>
        </div>
        <Button
          type="primary"
          onClick={() => {
            setEditing(null);
            setEditorOpen(true);
          }}
        >
          新建租户
        </Button>
      </div>

      <div className="rounded-[14px] border border-line bg-surface p-[18px]">
        <div className="mb-3.5 flex flex-wrap items-center gap-2.5">
          <Input.Search
            className="w-full md:w-[260px]"
            placeholder="搜索租户名称"
            allowClear
            defaultValue={q}
            onSearch={(value) => applyFilters(value.trim(), status)}
          />
          <Select
            value={status}
            style={{ width: 130 }}
            onChange={(value) => applyFilters(q, value)}
            options={[
              { value: "", label: "全部状态" },
              { value: "active", label: "启用" },
              { value: "disabled", label: "停用" },
            ]}
          />
          <div className="flex-1" />
          <span className="text-xs text-muted">
            本页 {rows.length} 条{nextCursor ? " · 还有更多" : ""}
          </span>
          <Button onClick={reload} loading={loading}>
            刷新
          </Button>
        </div>

        {error ? (
          <Alert
            type="error"
            showIcon
            message={error}
            style={{ marginBottom: 14 }}
          />
        ) : null}

        <Table<Tenant>
          rowKey="id"
          loading={loading}
          columns={columns}
          dataSource={rows}
          pagination={false}
          scroll={{ x: 710 }}
          locale={{ emptyText: "没有匹配的租户" }}
        />

        <div className="mt-3.5 flex items-center justify-end gap-2">
          <span className="mr-1.5 text-xs leading-6 text-muted">
            第 {pageIndex + 1} 页
          </span>
          <Button size="small" disabled={pageIndex === 0} onClick={goPrev}>
            上一页
          </Button>
          <Button size="small" disabled={nextCursor == null} onClick={goNext}>
            下一页
          </Button>
        </div>
      </div>

      <TenantEditor
        open={editorOpen}
        tenant={editing}
        onClose={() => setEditorOpen(false)}
        onSaved={reload}
      />

      <KeysDrawer
        open={keysTenant !== null}
        tenant={keysTenant}
        onClose={() => {
          setKeysTenant(null);
          reload();
        }}
      />
    </div>
  );
}
