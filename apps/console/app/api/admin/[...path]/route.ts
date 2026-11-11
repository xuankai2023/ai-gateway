import { NextResponse } from "next/server";
import { getSession, type Role } from "@/lib/auth";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

type Context = { params: { path: string[] } };

const MUTATING = new Set(["POST", "PUT", "PATCH", "DELETE"]);

// viewer 只读；operator 可改租户与 Key；admin 放行全部
function canWrite(role: Role, path: string[]): boolean {
  if (role === "admin") return true;
  if (role === "operator") return path[0] === "tenants";
  return false;
}

function sameOrigin(request: Request): boolean {
  const origin = request.headers.get("origin");
  if (!origin) return true;
  try {
    return new URL(origin).host === request.headers.get("host");
  } catch {
    return false;
  }
}

function fail(status: number, message: string) {
  return NextResponse.json({ error: { message } }, { status });
}

async function handle(request: Request, { params }: Context) {
  const session = getSession();
  if (!session) {
    return fail(401, "登录已过期，请重新登录");
  }

  const method = request.method.toUpperCase();
  if (MUTATING.has(method)) {
    if (!sameOrigin(request)) {
      return fail(403, "跨站请求被拒绝");
    }
    if (!canWrite(session.role, params.path)) {
      return fail(403, "当前账号没有写权限");
    }
  }

  const base = process.env.ADMIN_API_URL;
  const token = process.env.ADMIN_API_TOKEN;
  if (!base || !token) {
    return fail(503, "管理面地址或 token 未配置");
  }

  const incoming = new URL(request.url);
  const path = params.path.map(encodeURIComponent).join("/");
  const target = new URL(`/admin/v1/${path}`, base);
  target.search = incoming.search;

  const headers: Record<string, string> = {
    Accept: "application/json",
    "X-Admin-Token": token,
    "X-Admin-Actor": session.sub,
  };
  const contentType = request.headers.get("content-type");
  if (contentType) headers["Content-Type"] = contentType;

  try {
    const upstream = await fetch(target, {
      method,
      headers,
      body: MUTATING.has(method) ? await request.text() : undefined,
      cache: "no-store",
      redirect: "manual",
    });
    const body = await upstream.arrayBuffer();
    return new NextResponse(upstream.status === 204 || body.byteLength === 0 ? null : body, {
      status: upstream.status,
      headers: {
        "Content-Type": upstream.headers.get("content-type") ?? "application/json",
      },
    });
  } catch {
    return fail(502, "管理面暂时不可用");
  }
}

export async function GET(request: Request, context: Context) {
  return handle(request, context);
}

export async function POST(request: Request, context: Context) {
  return handle(request, context);
}

export async function PUT(request: Request, context: Context) {
  return handle(request, context);
}

export async function PATCH(request: Request, context: Context) {
  return handle(request, context);
}

export async function DELETE(request: Request, context: Context) {
  return handle(request, context);
}
