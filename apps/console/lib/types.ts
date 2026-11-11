export type Tenant = {
  id: number;
  name: string;
  tier: string;
  budget_monthly: number;
  status: string;
  rps_limit: number | null;
  tpm_limit: number | null;
  concurrency_limit: number | null;
  cache_enabled: boolean;
  created_at: string | null;
};

export type TenantPage = {
  data: Tenant[];
  next_cursor: number | null;
};

export type APIKey = {
  id: number;
  tenant_id: number;
  name: string;
  expires_at: string | null;
  retire_at: string | null;
  created_at: string | null;
};

export type CreateTenantInput = {
  name: string;
  tier: string;
  budget_monthly: number;
  rps_limit: number | null;
  tpm_limit: number | null;
  concurrency_limit: number | null;
};

export type UpdateTenantInput = Partial<CreateTenantInput> & {
  status?: "active" | "disabled";
  cache_enabled?: boolean;
};

export type CreateKeyInput = {
  name: string;
  expires_in_days: number;
};

export type IssuedKey = {
  id?: number;
  name?: string;
  api_key: string;
  expires_at?: string | null;
  retire_at?: string | null;
  grace_period_seconds?: number;
};
