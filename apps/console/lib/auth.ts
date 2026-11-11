import crypto from "node:crypto";
import { cookies } from "next/headers";
import { redirect } from "next/navigation";

export type Role = "viewer" | "operator" | "admin";

export type Session = {
  /** 用户名 */
  sub: string;
  role: Role;
  /** 过期时间（秒级时间戳） */
  exp: number;
};

export const SESSION_COOKIE = "gw_console_session";
export const SESSION_TTL = 8 * 60 * 60;

type User = {
  username: string;
  password_hash: string;
  role: Role;
};

const ROLES: Role[] = ["viewer", "operator", "admin"];

export function getUsers(): User[] {
  const raw = process.env.CONSOLE_USERS;
  if (!raw) {
    throw new Error("CONSOLE_USERS 未配置");
  }
  const parsed: unknown = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error("CONSOLE_USERS 必须是 JSON 数组");
  }
  return parsed.filter((item): item is User => {
    if (!item || typeof item !== "object") return false;
    const u = item as Record<string, unknown>;
    return (
      typeof u.username === "string" &&
      typeof u.password_hash === "string" &&
      ROLES.includes(u.role as Role)
    );
  });
}

// 校验 scrypt:<saltHex>:<hashHex> 格式的密码哈希（用冒号避免 .env 中 $ 被展开）
export function verifyPassword(password: string, stored: string): boolean {
  const [algo, saltHex, hashHex] = stored.split(":");
  if (algo !== "scrypt" || !saltHex || !hashHex) return false;
  const expected = Buffer.from(hashHex, "hex");
  if (expected.length === 0) return false;
  const actual = crypto.scryptSync(password, Buffer.from(saltHex, "hex"), expected.length);
  return actual.length === expected.length && crypto.timingSafeEqual(actual, expected);
}

export function authenticate(username: string, password: string): Session | null {
  const user = getUsers().find((u) => u.username === username);
  if (!user || !verifyPassword(password, user.password_hash)) return null;
  return {
    sub: user.username,
    role: user.role,
    exp: Math.floor(Date.now() / 1000) + SESSION_TTL,
  };
}

function secret(): string {
  const value = process.env.CONSOLE_SESSION_SECRET;
  if (!value || value.length < 24) {
    throw new Error("CONSOLE_SESSION_SECRET 未配置或长度不足 24 字符");
  }
  return value;
}

export function signSession(session: Session): string {
  const payload = Buffer.from(JSON.stringify(session)).toString("base64url");
  const sig = crypto.createHmac("sha256", secret()).update(payload).digest("base64url");
  return `${payload}.${sig}`;
}

export function verifySessionToken(token: string): Session | null {
  const [payload, sig] = token.split(".");
  if (!payload || !sig) return null;

  const expected = crypto
    .createHmac("sha256", secret())
    .update(payload)
    .digest("base64url");
  const given = Buffer.from(sig);
  const want = Buffer.from(expected);
  if (given.length !== want.length || !crypto.timingSafeEqual(given, want)) {
    return null;
  }

  try {
    const session = JSON.parse(Buffer.from(payload, "base64url").toString()) as Session;
    if (!session.sub || !ROLES.includes(session.role)) return null;
    if (typeof session.exp !== "number" || session.exp <= Math.floor(Date.now() / 1000)) {
      return null;
    }
    return session;
  } catch {
    return null;
  }
}

export function getSession(): Session | null {
  const token = cookies().get(SESSION_COOKIE)?.value;
  return token ? verifySessionToken(token) : null;
}

export function requireSession(): Session {
  const session = getSession();
  if (!session) redirect("/login");
  return session;
}
