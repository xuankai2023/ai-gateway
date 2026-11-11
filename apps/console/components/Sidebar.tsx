"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import type { Role } from "@/lib/auth";

const NAV = [
  { href: "/tenants", index: "01", label: "租户管理" },
  { href: "/gateway/config", index: "02", label: "网关配置" },
  { href: "/cost", index: "03", label: "成本看板" },
  { href: "/gray", index: "04", label: "灰度发布" },
  { href: "/monitor", index: "05", label: "可观测性" },
];

const ROLE_LABEL: Record<Role, string> = {
  admin: "管理员",
  operator: "操作员",
  viewer: "只读",
};

const NAV_ITEM = "flex items-center gap-2.5 rounded-[10px] px-3 py-2.5 text-sm text-[#c6d2cb] transition-colors hover:bg-white/[0.06]";

export default function Sidebar({ role }: { role: Role }) {
  const pathname = usePathname();

  return (
    <aside className="flex flex-wrap items-center gap-x-6 gap-y-3 bg-sidebar px-5 py-4 text-[#edf2ee] min-[900px]:sticky min-[900px]:top-0 min-[900px]:h-screen min-[900px]:flex-col min-[900px]:flex-nowrap min-[900px]:items-stretch min-[900px]:gap-y-0 min-[900px]:px-5 min-[900px]:py-7">
      <div>
        <div className="flex items-center gap-[10px]">
          <span className="grid h-[30px] w-[30px] place-items-center rounded-[9px] bg-brand text-[13px] font-bold text-white">
            AG
          </span>
          <span className="text-[13px] font-semibold tracking-[0.16em]">AI GATEWAY</span>
        </div>
        <div className="mt-1.5 text-[11px] tracking-[0.08em] text-sidebar-muted">
          CONSOLE · v0.1
        </div>
      </div>

      <nav className="hidden flex-col gap-1.5 min-[900px]:mt-[42px] min-[900px]:flex">
        <div className="mb-2 text-[10px] uppercase tracking-[0.2em] text-sidebar-muted">
          Workspace
        </div>
        {NAV.map((item) => {
          const active =
            !!pathname &&
            (pathname === item.href || pathname.startsWith(`${item.href}/`));
          if (item.href === "/tenants") {
            return (
              <Link
                key={item.href}
                href={item.href}
                className={`${NAV_ITEM} ${active ? "bg-[#22322a] text-white" : ""}`}
              >
                <span
                  className={`w-[18px] font-mono text-[11px] ${active ? "text-[#8fd3ae]" : "text-sidebar-muted"}`}
                >
                  {item.index}
                </span>
                {item.label}
              </Link>
            );
          }
          return (
            <div key={item.href} className={NAV_ITEM} aria-disabled>
              <span className="w-[18px] font-mono text-[11px] text-sidebar-muted">
                {item.index}
              </span>
              {item.label}
              <span className="ml-auto rounded-full border border-white/[0.14] px-1.5 py-px text-[10px] text-[#93a79b]">
                待建
              </span>
            </div>
          );
        })}
      </nav>

      <div className="hidden border-t border-white/10 pt-4 text-xs leading-[1.8] text-sidebar-muted min-[900px]:mt-auto min-[900px]:block">
        <div>
          角色 <strong className="font-medium text-[#dce6e0]">{ROLE_LABEL[role]}</strong>
        </div>
        <div>管理面仅内网 · 浏览器不直连</div>
      </div>
    </aside>
  );
}
