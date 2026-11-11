"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";

export default function LogoutButton() {
  const router = useRouter();
  const [loading, setLoading] = useState(false);

  async function logout() {
    setLoading(true);
    try {
      await fetch("/api/auth/logout", { method: "POST" });
      router.replace("/login");
      router.refresh();
    } finally {
      setLoading(false);
    }
  }

  return (
    <button
      type="button"
      className="cursor-pointer p-1 text-[13px] text-muted transition-colors hover:text-danger disabled:cursor-not-allowed disabled:opacity-50"
      onClick={logout}
      disabled={loading}
    >
      退出登录
    </button>
  );
}
