import { redirect } from "next/navigation";
import LoginForm from "@/components/LoginForm";
import { getSession } from "@/lib/auth";

export default function LoginPage() {
  if (getSession()) {
    redirect("/tenants");
  }

  return (
    <div className="grid min-h-screen grid-cols-1 min-[900px]:grid-cols-[minmax(0,1.1fr)_minmax(380px,0.9fr)]">
      <section className="relative hidden flex-col justify-between overflow-hidden bg-sidebar px-14 py-14 text-[#edf2ee] min-[900px]:flex">
        <div>
          <div className="flex items-center gap-[10px]">
            <span className="grid h-[30px] w-[30px] place-items-center rounded-[9px] bg-brand text-[13px] font-bold text-white">
              AG
            </span>
            <span className="text-[13px] font-semibold tracking-[0.16em]">
              AI GATEWAY
            </span>
          </div>
          <h1 className="mt-16 max-w-[520px] text-[clamp(34px,4vw,56px)] font-semibold leading-[1.06] tracking-[-0.03em]">
            把模型接入
            <br />
            变成一件
            <br />
            可治理的事。
          </h1>
          <p className="mt-7 max-w-[420px] text-sm leading-[1.8] text-[#9fb0a6]">
            统一路由、配额、账本与可观测性。控制台只做一件事：让你看清每个
            租户、每条渠道、每一分成本。
          </p>
        </div>
        <div
          aria-hidden
          className="pointer-events-none absolute -bottom-20 -right-[70px] h-[300px] w-[300px] rounded-full border border-white/[0.16] after:absolute after:inset-14 after:rounded-full after:border after:border-white/10 after:content-['']"
        />
      </section>

      <section className="flex items-center justify-center bg-canvas p-10">
        <div className="w-full max-w-[380px]">
          <h2 className="mb-1.5 text-2xl tracking-[-0.01em]">登录控制台</h2>
          <p className="mb-6 text-[13px] leading-[1.7] text-muted">
            使用运维账号进入。会话 8 小时有效。
          </p>
          <LoginForm />
          <div className="mt-[18px] text-xs leading-[1.7] text-muted">
            账号由 <span className="font-mono">CONSOLE_USERS</span> 配置，
            忘了密码可用 <span className="font-mono">pnpm hash-password</span> 重新生成哈希。
          </div>
        </div>
      </section>
    </div>
  );
}
