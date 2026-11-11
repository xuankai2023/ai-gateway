"use client";

import { useEffect, useState } from "react";
import { App, Form, Input, InputNumber, Modal, Select, Switch } from "antd";
import { createTenant, updateTenant } from "@/lib/api/client";
import type { Tenant } from "@/lib/types";

type FormValues = {
  name: string;
  tier: string;
  budget_monthly: number | null;
  rps_limit: number | null;
  tpm_limit: number | null;
  concurrency_limit: number | null;
  cache_enabled: boolean;
};

const TIERS = [
  { value: "free", label: "free" },
  { value: "pro", label: "pro" },
  { value: "enterprise", label: "enterprise" },
];

export default function TenantEditor({
  open,
  tenant,
  onClose,
  onSaved,
}: {
  open: boolean;
  tenant: Tenant | null;
  onClose: () => void;
  onSaved: () => void;
}) {
  const { message } = App.useApp();
  const [form] = Form.useForm<FormValues>();
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!open) return;
    if (tenant) {
      form.setFieldsValue({
        name: tenant.name,
        tier: tenant.tier,
        budget_monthly: tenant.budget_monthly,
        rps_limit: tenant.rps_limit,
        tpm_limit: tenant.tpm_limit,
        concurrency_limit: tenant.concurrency_limit,
        cache_enabled: tenant.cache_enabled,
      });
    } else {
      form.resetFields();
      form.setFieldsValue({
        tier: "free",
        budget_monthly: 0,
        cache_enabled: false,
      });
    }
  }, [open, tenant, form]);

  async function submit() {
    const values = await form.validateFields();
    setSaving(true);
    try {
      const payload = {
        name: values.name.trim(),
        tier: values.tier,
        budget_monthly: values.budget_monthly ?? 0,
        rps_limit: values.rps_limit ?? null,
        tpm_limit: values.tpm_limit ?? null,
        concurrency_limit: values.concurrency_limit ?? null,
      };
      if (tenant) {
        await updateTenant(tenant.id, {
          ...payload,
          cache_enabled: values.cache_enabled,
        });
        message.success("租户已更新");
      } else {
        await createTenant(payload);
        message.success("租户已创建");
      }
      onSaved();
      onClose();
    } catch (err) {
      message.error(err instanceof Error ? err.message : "保存失败");
    } finally {
      setSaving(false);
    }
  }

  const tierOptions = TIERS.some((t) => t.value === tenant?.tier)
    ? TIERS
    : [...TIERS, { value: tenant?.tier ?? "free", label: tenant?.tier ?? "free" }];

  return (
    <Modal
      open={open}
      title={tenant ? `编辑租户 · ${tenant.name}` : "新建租户"}
      okText={tenant ? "保存" : "创建"}
      cancelText="取消"
      confirmLoading={saving}
      onOk={submit}
      onCancel={onClose}
      destroyOnClose
      width={560}
    >
      <Form<FormValues>
        form={form}
        layout="vertical"
        requiredMark={false}
        style={{ marginTop: 20 }}
      >
        <Form.Item
          label="租户名称"
          name="name"
          rules={[{ required: true, message: "请输入租户名称" }]}
        >
          <Input placeholder="例如 acme" maxLength={64} />
        </Form.Item>

        <Form.Item label="套餐" name="tier">
          <Select options={tierOptions} />
        </Form.Item>

        <Form.Item
          label="月度预算（USD）"
          name="budget_monthly"
          extra="0 表示不限额"
        >
          <InputNumber
            min={0}
            precision={2}
            step={10}
            addonBefore="$"
            style={{ width: "100%" }}
          />
        </Form.Item>

        <div className="grid gap-3 md:grid-cols-3">
          <Form.Item label="RPS 上限" name="rps_limit" extra="留空不限">
            <InputNumber min={1} style={{ width: "100%" }} />
          </Form.Item>
          <Form.Item label="TPM 上限" name="tpm_limit" extra="留空不限">
            <InputNumber min={1} style={{ width: "100%" }} />
          </Form.Item>
          <Form.Item label="并发上限" name="concurrency_limit" extra="留空不限">
            <InputNumber min={1} style={{ width: "100%" }} />
          </Form.Item>
        </div>

        {tenant ? (
          <Form.Item
            label="语义缓存"
            name="cache_enabled"
            valuePropName="checked"
          >
            <Switch checkedChildren="开" unCheckedChildren="关" />
          </Form.Item>
        ) : null}
      </Form>
    </Modal>
  );
}
