import type {
  APIKey,
  CreateKeyInput,
  CreateTenantInput,
  IssuedKey,
  TenantPage,
  UpdateTenantInput,
} from "@/lib/types";

type ErrorBody = { error?: { message?: string } };

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`/api/admin${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
    cache: "no-store",
  });

  const text = await response.text();
  let payload: unknown = null;
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = null;
    }
  }

  if (!response.ok) {
    const message = (payload as ErrorBody | null)?.error?.message;
    throw new Error(message ?? `请求失败 (${response.status})`);
  }
  return payload as T;
}

export function listTenants(params: {
  cursor: number | null;
  limit: number;
  q: string;
  status: string;
}): Promise<TenantPage> {
  const search = new URLSearchParams({ limit: String(params.limit) });
  if (params.cursor) search.set("cursor", String(params.cursor));
  if (params.q) search.set("q", params.q);
  if (params.status) search.set("status", params.status);
  return request<TenantPage>(`/tenants?${search.toString()}`);
}

export function createTenant(input: CreateTenantInput) {
  return request<{ data: { id: number } }>("/tenants", {
    method: "POST",
    body: JSON.stringify(input),
  });
}

export function updateTenant(id: number, patch: UpdateTenantInput) {
  return request<{ data: { id: number } }>(`/tenants/${id}`, {
    method: "PATCH",
    body: JSON.stringify(patch),
  });
}

export function listKeys(tenantId: number) {
  return request<{ data: APIKey[] }>(`/tenants/${tenantId}/keys`);
}

export function createKey(tenantId: number, input: CreateKeyInput) {
  return request<{ data: IssuedKey }>(`/tenants/${tenantId}/keys`, {
    method: "POST",
    body: JSON.stringify(input),
  });
}

export function revokeKey(id: number) {
  return request<{ data: { id: number; revoked: boolean } }>(`/keys/${id}`, {
    method: "DELETE",
  });
}

export function rotateKey(id: number) {
  return request<{ data: IssuedKey }>(`/keys/${id}/rotate`, {
    method: "POST",
    body: JSON.stringify({}),
  });
}
