"use client";

import { useCallback, useEffect, useState } from "react";
import {
  Alert,
  App,
  Button,
  Drawer,
  Form,
  Input,
  InputNumber,
  Popconfirm,
  Space,
  Table,
  Typography,
} from "antd";
import type { ColumnsType } from "antd/es/table";
import { createKey, listKeys, revokeKey, rotateKey } from "@/lib/api/client";
import type { APIKey, IssuedKey, Tenant } from "@/lib/types";

function formatTime(value: string | null) {
  if (!value) return "—";
  return new Date(value).toLocaleString("zh-CN", { hour12: false });
}

export default function KeysDrawer({
  tenant,
  open,
  onClose,
}: {
  tenant: Tenant | null;
  open: boolean;
  onClose: () => void;
}) {
  const { message } = App.useApp();
  const [form] = Form.useForm<{ name: string; expires_in_days: number | null }>();
  const [keys, setKeys] = useState<APIKey[]>([]);
  const [issued, setIssued] = useState<IssuedKey | null>(null);
  const [loading, setLoading] = useState(false);
  const [creating, setCreating] = useState(false);
  const [busyId, setBusyId] = useState<number | null>(null);

  const load = useCallback(async () => {
    if (!tenant) return;
    setLoading(true);
    try {
      const result = await listKeys(tenant.id);
      setKeys(result.data ?? []);
    } catch (err) {
      message.error(err instanceof Error ? err.message : "Key 列表加载失败");
    } finally {
      setLoading(false);
    }
  }, [tenant, message]);

  useEffect(() => {
    if (!open) return;
    setIssued(null);
    form.resetFields();
    void load();
  }, [open, form, load]);

  async function submit() {
    if (!tenant) return;
    const values = await form.validateFields();
    setCreating(true);
    try {
      const result = await createKey(tenant.id, {
        name: values.name.trim(),
        expires_in_days: values.expires_in_days ?? 0,
      });
      setIssued(result.data);
      form.resetFields();
      message.success("Key 已签发");
      await load();
    } catch (err) {
      message.error(err instanceof Error ? err.message : "签发失败");
    } finally {
      setCreating(false);
    }
  }

  async function rotate(id: number) {
    setBusyId(id);
    try {
      const result = await rotateKey(id);
      setIssued(result.data);
      message.success("已轮换，旧 Key 进入 24 小时宽限期");
      await load();
    } catch (err) {
      message.error(err instanceof Error ? err.message : "轮换失败");
    } finally {
      setBusyId(null);
    }
  }

  async function revoke(id: number) {
    setBusyId(id);
    try {
      await revokeKey(id);
      message.success("Key 已吊销");
      await load();
    } catch (err) {
      message.error(err instanceof Error ? err.message : "吊销失败");
    } finally {
      setBusyId(null);
    }
  }

  async function copyKey() {
    if (!issued) return;
    try {
      await navigator.clipboard.writeText(issued.api_key);
      message.success("已复制到剪贴板");
    } catch {
      message.warning("复制失败，请手动选择文本");
    }
  }

  const columns: ColumnsType<APIKey> = [
    {
      title: "名称",
      dataIndex: "name",
      width: 220,
      ellipsis: true,
      render: (_, row) => (
        <div title={`${row.name} #${row.id}`}>
          <div className="font-medium">{row.name}</div>
          <div className="mt-0.5 font-mono text-xs text-muted">#{row.id}</div>
        </div>
      ),
    },
    {
      title: "创建时间",
      dataIndex: "created_at",
      width: 160,
      render: (value: string | null) => (
        <span className="font-mono text-xs">{formatTime(value)}</span>
      ),
    },
    {
      title: "到期",
      dataIndex: "expires_at",
      width: 100,
      render: (value: string | null) =>
        value ? (
          <span className="font-mono text-xs" title={formatTime(value)}>
            {formatTime(value).slice(0, 10)}
          </span>
        ) : (
          "永久"
        ),
    },
    {
      title: "状态",
      dataIndex: "retire_at",
      width: 100,
      render: (value: string | null) =>
        value ? <span title={formatTime(value)}>退役中</span> : "正常",
    },
    {
      title: "操作",
      key: "actions",
      width: 130,
      render: (_, row) => (
        <Space size={2}>
          <Popconfirm
            title="轮换 Key"
            description="将立即签发新 Key，旧 Key 默认保留 24 小时。"
            okText="轮换"
            cancelText="取消"
            onConfirm={() => rotate(row.id)}
            disabled={Boolean(row.retire_at)}
          >
            <Button type="link" size="small" disabled={Boolean(row.retire_at)}>
              轮换
            </Button>
          </Popconfirm>
          <Popconfirm
            title="吊销 Key"
            description="吊销后立即失效，不可恢复。"
            okText="吊销"
            cancelText="取消"
            okButtonProps={{ danger: true }}
            onConfirm={() => revoke(row.id)}
          >
            <Button type="link" size="small" danger loading={busyId === row.id}>
              吊销
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <Drawer
      open={open}
      onClose={onClose}
      width={780}
      title={tenant ? `API Keys · ${tenant.name}` : "API Keys"}
      destroyOnClose
    >
      {issued ? (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 18 }}
          message="新 Key 只显示这一次，请立即保存"
          description={
            <div style={{ marginTop: 8 }}>
              <Typography.Paragraph
                className="mb-2 break-all font-mono text-xs"
                copyable={{ text: issued.api_key, onCopy: () => message.success("已复制") }}
              >
                {issued.api_key}
              </Typography.Paragraph>
              <Button size="small" onClick={copyKey}>
                复制 Key
              </Button>
            </div>
          }
        />
      ) : null}

      <Form
        form={form}
        layout="inline"
        onFinish={submit}
        style={{ marginBottom: 18, gap: 8 }}
      >
        <Form.Item
          name="name"
          rules={[{ required: true, message: "请输入名称" }]}
          style={{ flex: 1, minWidth: 160 }}
        >
          <Input placeholder="Key 名称，例如 ci-prod" />
        </Form.Item>
        <Form.Item name="expires_in_days">
          <InputNumber min={0} placeholder="有效天数" style={{ width: 120 }} />
        </Form.Item>
        <Form.Item>
          <Button type="primary" htmlType="submit" loading={creating}>
            签发
          </Button>
        </Form.Item>
      </Form>

      <Table<APIKey>
        rowKey="id"
        size="small"
        loading={loading}
        columns={columns}
        dataSource={keys}
        pagination={false}
        locale={{ emptyText: "还没有 API Key" }}
      />
    </Drawer>
  );
}
