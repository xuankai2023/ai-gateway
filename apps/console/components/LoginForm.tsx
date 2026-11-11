"use client";

import { useState } from "react";
import { Alert, Button, Form, Input } from "antd";
import { useRouter } from "next/navigation";

type Values = { username: string; password: string };

export default function LoginForm() {
  const router = useRouter();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function onFinish(values: Values) {
    setLoading(true);
    setError("");
    try {
      const response = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(values),
      });
      const payload = await response.json().catch(() => null);
      if (!response.ok) {
        setError(payload?.error?.message ?? `登录失败 (${response.status})`);
        return;
      }
      router.replace("/tenants");
      router.refresh();
    } catch {
      setError("网络异常，请稍后重试");
    } finally {
      setLoading(false);
    }
  }

  return (
    <Form<Values>
      layout="vertical"
      requiredMark={false}
      onFinish={onFinish}
      size="large"
    >
      {error ? (
        <Alert
          type="error"
          showIcon
          message={error}
          style={{ marginBottom: 18 }}
        />
      ) : null}
      <Form.Item
        label="用户名"
        name="username"
        rules={[{ required: true, message: "请输入用户名" }]}
      >
        <Input autoComplete="username" placeholder="admin" autoFocus />
      </Form.Item>
      <Form.Item
        label="密码"
        name="password"
        rules={[{ required: true, message: "请输入密码" }]}
      >
        <Input.Password autoComplete="current-password" placeholder="••••••••" />
      </Form.Item>
      <Button type="primary" htmlType="submit" block loading={loading}>
        进入控制台
      </Button>
    </Form>
  );
}
