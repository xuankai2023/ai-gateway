"use client";

import { App as AntdApp, ConfigProvider } from "antd";
import zhCN from "antd/locale/zh_CN";
import type { ReactNode } from "react";

export default function Providers({ children }: { children: ReactNode }) {
  return (
    <ConfigProvider
      locale={zhCN}
      theme={{
        token: {
          colorPrimary: "#2e6b4f",
          colorInfo: "#2e6b4f",
          colorLink: "#2e6b4f",
          colorText: "#17201c",
          colorTextSecondary: "#6c7570",
          colorBorder: "#e3e1d8",
          colorBgLayout: "#f3f2ed",
          borderRadius: 10,
          controlHeight: 36,
          fontFamily:
            '"Inter", "SF Pro Text", "PingFang SC", "Hiragino Sans GB", system-ui, -apple-system, "Segoe UI", Roboto, sans-serif',
        },
        components: {
          Table: {
            headerBg: "#faf9f5",
            headerColor: "#6c7570",
            rowHoverBg: "#fafaf6",
            cellPaddingBlock: 14,
          },
          Button: {
            primaryShadow: "none",
            defaultShadow: "none",
          },
        },
      }}
    >
      <AntdApp>{children}</AntdApp>
    </ConfigProvider>
  );
}
