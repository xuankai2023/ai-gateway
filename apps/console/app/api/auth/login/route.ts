import { NextResponse } from "next/server";
import {
  authenticate,
  SESSION_COOKIE,
  SESSION_TTL,
  signSession,
} from "@/lib/auth";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function POST(request: Request) {
  let body: { username?: string; password?: string };
  try {
    body = await request.json();
  } catch {
    return NextResponse.json(
      { error: { message: "请求格式错误" } },
      { status: 400 },
    );
  }

  const username = body.username?.trim();
  const password = body.password ?? "";
  if (!username || !password) {
    return NextResponse.json(
      { error: { message: "请输入用户名和密码" } },
      { status: 400 },
    );
  }

  let session;
  try {
    session = authenticate(username, password);
  } catch (err) {
    const message = err instanceof Error ? err.message : "控制台用户配置错误";
    return NextResponse.json({ error: { message } }, { status: 503 });
  }

  if (!session) {
    return NextResponse.json(
      { error: { message: "用户名或密码错误" } },
      { status: 401 },
    );
  }

  let token: string;
  try {
    token = signSession(session);
  } catch (err) {
    const message = err instanceof Error ? err.message : "会话签名失败";
    return NextResponse.json({ error: { message } }, { status: 503 });
  }

  const response = NextResponse.json({
    data: { username: session.sub, role: session.role },
  });
  response.cookies.set(SESSION_COOKIE, token, {
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    path: "/",
    maxAge: SESSION_TTL,
  });
  return response;
}
