import LogoutButton from "@/components/LogoutButton";
import Sidebar from "@/components/Sidebar";
import { requireSession, type Role } from "@/lib/auth";

const ROLE_LABEL: Record<Role, string> = {
  admin: "ADMIN",
  operator: "OPERATOR",
  viewer: "VIEWER",
};

export default function ConsoleLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  const session = requireSession();

  return (
    <div className="grid min-h-screen grid-cols-1 min-[900px]:grid-cols-[236px_minmax(0,1fr)]">
      <Sidebar role={session.role} />
      <div className="min-w-0 px-5 pb-14 pt-5 md:px-9">
        <header className="flex flex-wrap items-center justify-between gap-4 border-b border-line pb-5">
          <div className="text-xs tracking-[0.12em] text-muted">
            AI Gateway <b className="font-medium text-ink">/ Console</b>
          </div>
          <div className="flex items-center gap-3 text-[13px]">
            <span className="rounded-full bg-brand-soft px-2 py-[3px] text-[10px] uppercase tracking-[0.1em] text-brand">
              {ROLE_LABEL[session.role]}
            </span>
            <span>{session.sub}</span>
            <LogoutButton />
          </div>
        </header>
        {children}
      </div>
    </div>
  );
}
