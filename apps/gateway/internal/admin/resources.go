// models / abilities / channel_keys / gray_rules 管理
package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/apperr"
	"ai-gateway/pkg/ledger"
	"github.com/gin-gonic/gin"
)

// ---------- models ----------

func listModels(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		q := `SELECT id, tenant_id, model_name, provider, provider_model, is_active, input_price_1k, output_price_1k, pricing_model, price_version FROM models`
		args := []any{}
		if tid := c.Query("tenant_id"); tid != "" {
			q += ` WHERE tenant_id = ?`
			args = append(args, tid)
		}
		q += ` ORDER BY id LIMIT 500`
		rows, err := pool.QueryContext(ctx, q, args...)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_query_failed", err.Error())
			return
		}
		defer rows.Close()
		out := []map[string]any{}
		for rows.Next() {
			var (
				id, tenantID                int64
				model, provider             string
				providerModel               string
				isActive                    bool
				inPriceMicro, outPriceMicro int64
				pricingModel                string
				priceVersion                int
			)
			if err := rows.Scan(&id, &tenantID, &model, &provider, &providerModel, &isActive,
				&inPriceMicro, &outPriceMicro, &pricingModel, &priceVersion); err != nil {
				apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_scan_failed", err.Error())
				return
			}
			out = append(out, map[string]any{
				"id": id, "tenant_id": tenantID, "model_name": model, "provider": provider,
				"provider_model": providerModel, "is_active": isActive,
				"input_price_1k":       ledger.MicroUSD(inPriceMicro).Dollars(),
				"output_price_1k":      ledger.MicroUSD(outPriceMicro).Dollars(),
				"input_price_1k_micro": inPriceMicro, "output_price_1k_micro": outPriceMicro,
				"pricing_model": pricingModel, "price_version": priceVersion,
			})
		}
		c.JSON(http.StatusOK, gin.H{"data": out})
	}
}

// createModelReq 创建模型路由入参
type createModelReq struct {
	TenantID      int64           `json:"tenant_id" binding:"required"`
	ModelName     string          `json:"model_name" binding:"required"`
	Provider      string          `json:"provider" binding:"required"`
	ProviderModel string          `json:"provider_model" binding:"required"`
	InputPrice1K  float64         `json:"input_price_1k"`
	OutputPrice1K float64         `json:"output_price_1k"`
	PricingModel  string          `json:"pricing_model"`
	PricingConfig json.RawMessage `json:"pricing_config"`
	PriceVersion  int             `json:"price_version"`
}

func createModel(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req createModelReq
		if !bindJSON(c, &req) {
			return
		}
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		if req.PricingModel == "" {
			req.PricingModel = "linear"
		}
		if req.PriceVersion <= 0 {
			req.PriceVersion = 1
		}
		res, err := pool.ExecContext(ctx,
			`INSERT INTO models (tenant_id, model_name, provider, provider_model, input_price_1k, output_price_1k, pricing_model, pricing_config, price_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			req.TenantID, req.ModelName, req.Provider, req.ProviderModel,
			int64(ledger.FromDollars(req.InputPrice1K)), int64(ledger.FromDollars(req.OutputPrice1K)),
			req.PricingModel, nullableJSON(req.PricingConfig), req.PriceVersion)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_insert_failed", err.Error())
			return
		}
		id, _ := res.LastInsertId()
		afterWrite(c, a, "model.create", req, "model")
		c.JSON(http.StatusCreated, gin.H{"data": gin.H{"id": id, "model_name": req.ModelName}})
	}
}

func updateModel(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, ok := pathID(c)
		if !ok {
			return
		}
		var body map[string]any
		if !bindJSON(c, &body) {
			return
		}
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		setSQL, args, err := buildUpdate("models", body, map[string]func(any) (any, error){
			"provider_model":  convString,
			"is_active":       convBool,
			"input_price_1k":  convUSDToMicro,
			"output_price_1k": convUSDToMicro,
			"pricing_model":   convString,
			"pricing_config":  convJSON,
			"price_version":   convInt,
		})
		if err != nil {
			apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "invalid_update_fields", err.Error())
			return
		}
		args = append(args, id)
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		res, err := pool.ExecContext(ctx, setSQL, args...)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_update_failed", err.Error())
			return
		}
		if n, _ := res.RowsAffected(); n == 0 {
			apperr.Abort(c, http.StatusNotFound, "not_found", "model_not_found", "model not found")
			return
		}
		afterWrite(c, a, "model.update", map[string]any{"id": id, "fields": sortedKeys(body)}, "model")
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"id": id, "updated": sortedKeys(body)}})
	}
}

func deleteModel(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, ok := pathID(c)
		if !ok {
			return
		}
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		res, err := pool.ExecContext(ctx, `DELETE FROM models WHERE id = ?`, id)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_delete_failed", err.Error())
			return
		}
		if n, _ := res.RowsAffected(); n == 0 {
			apperr.Abort(c, http.StatusNotFound, "not_found", "model_not_found", "model not found")
			return
		}
		afterWrite(c, a, "model.delete", map[string]any{"id": id}, "model")
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"id": id, "deleted": true}})
	}
}

// convUSDToMicro 管理面入参美元 → μUSD（models 价格列 012 migration 后为 BIGINT）
func convUSDToMicro(v any) (any, error) {
	f, ok := v.(float64)
	if !ok {
		return nil, fmt.Errorf("must be number, got %v", v)
	}
	return int64(ledger.FromDollars(f)), nil
}

// convJSON 把 JSON 对象 / 数组 / 字符串序列化为 JSON 文本（pricing_config）
func convJSON(v any) (any, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case string:
		return x, nil
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return nil, fmt.Errorf("must be valid json: %w", err)
		}
		return string(b), nil
	}
}

// nullableJSON 空 RawMessage 写 NULL，避免 MySQL JSON 列拒绝空字节串
func nullableJSON(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	return []byte(raw)
}

// ---------- abilities ----------

func listAbilities(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		q := `SELECT id, tenant_id, model_name, provider, provider_model, priority, weight, enabled, input_price_1k, output_price_1k FROM abilities`
		args := []any{}
		sep := ` WHERE `
		if tid := c.Query("tenant_id"); tid != "" {
			q += sep + `tenant_id = ?`
			args = append(args, tid)
			sep = ` AND `
		}
		if m := c.Query("model_name"); m != "" {
			q += sep + `model_name = ?`
			args = append(args, m)
		}
		q += ` ORDER BY id LIMIT 500`
		rows, err := pool.QueryContext(ctx, q, args...)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_query_failed", err.Error())
			return
		}
		defer rows.Close()
		out := []map[string]any{}
		for rows.Next() {
			var (
				id, tenantID      int64
				model, provider   string
				providerModel     string
				priority, weight  int
				enabled           bool
				inPrice, outPrice float64
			)
			if err := rows.Scan(&id, &tenantID, &model, &provider, &providerModel, &priority, &weight, &enabled, &inPrice, &outPrice); err != nil {
				apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_scan_failed", err.Error())
				return
			}
			out = append(out, map[string]any{
				"id": id, "tenant_id": tenantID, "model_name": model, "provider": provider,
				"provider_model": providerModel, "priority": priority, "weight": weight,
				"enabled": enabled, "input_price_1k": inPrice, "output_price_1k": outPrice,
			})
		}
		c.JSON(http.StatusOK, gin.H{"data": out})
	}
}

// createAbilityReq 创建渠道入参
type createAbilityReq struct {
	TenantID      int64   `json:"tenant_id" binding:"required"`
	ModelName     string  `json:"model_name" binding:"required"`
	Provider      string  `json:"provider" binding:"required"`
	ProviderModel string  `json:"provider_model" binding:"required"`
	Priority      int     `json:"priority"`
	Weight        int     `json:"weight"`
	InputPrice1K  float64 `json:"input_price_1k"`
	OutputPrice1K float64 `json:"output_price_1k"`
	FallbackModel *string `json:"fallback_model"`
}

func createAbility(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req createAbilityReq
		if !bindJSON(c, &req) {
			return
		}
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		priority := req.Priority
		if priority == 0 {
			priority = 100
		}
		weight := req.Weight
		if weight == 0 {
			weight = 1
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		res, err := pool.ExecContext(ctx,
			`INSERT INTO abilities (tenant_id, model_name, provider, provider_model, priority, weight, input_price_1k, output_price_1k, fallback_model) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			req.TenantID, req.ModelName, req.Provider, req.ProviderModel, priority, weight, req.InputPrice1K, req.OutputPrice1K, req.FallbackModel)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_insert_failed", err.Error())
			return
		}
		id, _ := res.LastInsertId()
		afterWrite(c, a, "ability.create", req, "abilities")
		c.JSON(http.StatusCreated, gin.H{"data": gin.H{"id": id}})
	}
}

func updateAbility(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, ok := pathID(c)
		if !ok {
			return
		}
		var body map[string]any
		if !bindJSON(c, &body) {
			return
		}
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		setSQL, args, err := buildUpdate("abilities", body, map[string]func(any) (any, error){
			"priority":        convInt,
			"weight":          convInt,
			"enabled":         convBool,
			"input_price_1k":  convFloat,
			"output_price_1k": convFloat,
			"fallback_model":  convString,
			"provider_model":  convString,
		})
		if err != nil {
			apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "invalid_update_fields", err.Error())
			return
		}
		args = append(args, id)
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		res, err := pool.ExecContext(ctx, setSQL, args...)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_update_failed", err.Error())
			return
		}
		if n, _ := res.RowsAffected(); n == 0 {
			apperr.Abort(c, http.StatusNotFound, "not_found", "ability_not_found", "ability not found")
			return
		}
		afterWrite(c, a, "ability.update", map[string]any{"id": id, "fields": sortedKeys(body)}, "abilities")
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"id": id, "updated": sortedKeys(body)}})
	}
}

func deleteAbility(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, ok := pathID(c)
		if !ok {
			return
		}
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		res, err := pool.ExecContext(ctx, `DELETE FROM abilities WHERE id = ?`, id)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_delete_failed", err.Error())
			return
		}
		if n, _ := res.RowsAffected(); n == 0 {
			apperr.Abort(c, http.StatusNotFound, "not_found", "ability_not_found", "ability not found")
			return
		}
		afterWrite(c, a, "ability.delete", map[string]any{"id": id}, "abilities")
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"id": id, "deleted": true}})
	}
}

// ---------- channel_keys ----------

func listChannelKeys(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		q := `SELECT id, channel_id, key_ref, weight, enabled, consecutive_failures, disabled_until, disabled_reason FROM channel_keys`
		args := []any{}
		if cid := c.Query("channel_id"); cid != "" {
			q += ` WHERE channel_id = ?`
			args = append(args, cid)
		}
		q += ` ORDER BY id LIMIT 500`
		rows, err := pool.QueryContext(ctx, q, args...)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_query_failed", err.Error())
			return
		}
		defer rows.Close()
		out := []map[string]any{}
		for rows.Next() {
			var (
				id, channelID int64
				keyRef        string
				weight        int
				enabled       bool
				failures      int
				disabledUntil *string
				reason        *string
			)
			if err := rows.Scan(&id, &channelID, &keyRef, &weight, &enabled, &failures, &disabledUntil, &reason); err != nil {
				apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_scan_failed", err.Error())
				return
			}
			out = append(out, map[string]any{
				"id": id, "channel_id": channelID, "key_ref": keyRef, "weight": weight,
				"enabled": enabled, "consecutive_failures": failures,
				"disabled_until": disabledUntil, "disabled_reason": reason,
			})
		}
		c.JSON(http.StatusOK, gin.H{"data": out})
	}
}

// createChannelKeyReq 新增渠道 key 入参（key_ref 只接受 env:// / vault:// 引用，拒绝明文）
type createChannelKeyReq struct {
	ChannelID int64  `json:"channel_id" binding:"required"`
	KeyRef    string `json:"key_ref" binding:"required"`
	Weight    int    `json:"weight"`
}

func createChannelKey(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req createChannelKeyReq
		if !bindJSON(c, &req) {
			return
		}
		if !isValidKeyRef(req.KeyRef) {
			apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "invalid_key_ref",
				"key_ref must be env://VAR_NAME or vault://path (plaintext refused)")
			return
		}
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		weight := req.Weight
		if weight == 0 {
			weight = 1
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		res, err := pool.ExecContext(ctx,
			`INSERT INTO channel_keys (channel_id, key_ref, weight) VALUES (?, ?, ?)`,
			req.ChannelID, req.KeyRef, weight)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_insert_failed", err.Error())
			return
		}
		id, _ := res.LastInsertId()
		afterWrite(c, a, "channel_key.create", req, "channel_keys")
		c.JSON(http.StatusCreated, gin.H{"data": gin.H{"id": id}})
	}
}

func updateChannelKey(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, ok := pathID(c)
		if !ok {
			return
		}
		var body map[string]any
		if !bindJSON(c, &body) {
			return
		}
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		setSQL, args, err := buildUpdate("channel_keys", body, map[string]func(any) (any, error){
			"weight":  convInt,
			"enabled": convBool,
		})
		if err != nil {
			apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "invalid_update_fields", err.Error())
			return
		}
		args = append(args, id)
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		res, err := pool.ExecContext(ctx, setSQL, args...)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_update_failed", err.Error())
			return
		}
		if n, _ := res.RowsAffected(); n == 0 {
			apperr.Abort(c, http.StatusNotFound, "not_found", "channel_key_not_found", "channel key not found")
			return
		}
		afterWrite(c, a, "channel_key.update", map[string]any{"id": id, "fields": sortedKeys(body)}, "channel_keys")
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"id": id, "updated": sortedKeys(body)}})
	}
}

func deleteChannelKey(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, ok := pathID(c)
		if !ok {
			return
		}
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		res, err := pool.ExecContext(ctx, `DELETE FROM channel_keys WHERE id = ?`, id)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_delete_failed", err.Error())
			return
		}
		if n, _ := res.RowsAffected(); n == 0 {
			apperr.Abort(c, http.StatusNotFound, "not_found", "channel_key_not_found", "channel key not found")
			return
		}
		afterWrite(c, a, "channel_key.delete", map[string]any{"id": id}, "channel_keys")
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"id": id, "deleted": true}})
	}
}

// isValidKeyRef key_ref 只接受引用 scheme（阶段 16：拒绝明文落库）
func isValidKeyRef(ref string) bool {
	return strings.HasPrefix(ref, "env://") || strings.HasPrefix(ref, "vault://")
}

// ---------- gray_rules ----------

func listGrayRules(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		q := `SELECT id, tenant_id, model_name, target_model, weight, enabled FROM gray_rules`
		args := []any{}
		if tid := c.Query("tenant_id"); tid != "" {
			q += ` WHERE tenant_id = ?`
			args = append(args, tid)
		}
		q += ` ORDER BY id LIMIT 500`
		rows, err := pool.QueryContext(ctx, q, args...)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_query_failed", err.Error())
			return
		}
		defer rows.Close()
		out := []map[string]any{}
		for rows.Next() {
			var (
				id, tenantID  int64
				model, target string
				weight        int
				enabled       bool
			)
			if err := rows.Scan(&id, &tenantID, &model, &target, &weight, &enabled); err != nil {
				apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_scan_failed", err.Error())
				return
			}
			out = append(out, map[string]any{
				"id": id, "tenant_id": tenantID, "model_name": model,
				"target_model": target, "weight": weight, "enabled": enabled,
			})
		}
		c.JSON(http.StatusOK, gin.H{"data": out})
	}
}

// createGrayRuleReq 创建灰度规则入参
type createGrayRuleReq struct {
	TenantID    int64  `json:"tenant_id" binding:"required"`
	ModelName   string `json:"model_name" binding:"required"`
	TargetModel string `json:"target_model" binding:"required"`
	Weight      int    `json:"weight"`
	Enabled     *bool  `json:"enabled"`
}

func createGrayRule(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req createGrayRuleReq
		if !bindJSON(c, &req) {
			return
		}
		if req.Weight < 0 || req.Weight > 100 {
			apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "invalid_weight", "weight must be in [0,100]")
			return
		}
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		enabled := 1
		if req.Enabled != nil && !*req.Enabled {
			enabled = 0
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		res, err := pool.ExecContext(ctx,
			`INSERT INTO gray_rules (tenant_id, model_name, target_model, weight, enabled) VALUES (?, ?, ?, ?, ?)`,
			req.TenantID, req.ModelName, req.TargetModel, req.Weight, enabled)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_insert_failed", err.Error())
			return
		}
		id, _ := res.LastInsertId()
		afterWrite(c, a, "gray_rule.create", req, "gray")
		c.JSON(http.StatusCreated, gin.H{"data": gin.H{"id": id}})
	}
}

func updateGrayRule(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, ok := pathID(c)
		if !ok {
			return
		}
		var body map[string]any
		if !bindJSON(c, &body) {
			return
		}
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		setSQL, args, err := buildUpdate("gray_rules", body, map[string]func(any) (any, error){
			"target_model": convString,
			"weight":       convInt,
			"enabled":      convBool,
		})
		if err != nil {
			apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "invalid_update_fields", err.Error())
			return
		}
		args = append(args, id)
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		res, err := pool.ExecContext(ctx, setSQL, args...)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_update_failed", err.Error())
			return
		}
		if n, _ := res.RowsAffected(); n == 0 {
			apperr.Abort(c, http.StatusNotFound, "not_found", "gray_rule_not_found", "gray rule not found")
			return
		}
		afterWrite(c, a, "gray_rule.update", map[string]any{"id": id, "fields": sortedKeys(body)}, "gray")
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"id": id, "updated": sortedKeys(body)}})
	}
}

func deleteGrayRule(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, ok := pathID(c)
		if !ok {
			return
		}
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		res, err := pool.ExecContext(ctx, `DELETE FROM gray_rules WHERE id = ?`, id)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_delete_failed", err.Error())
			return
		}
		if n, _ := res.RowsAffected(); n == 0 {
			apperr.Abort(c, http.StatusNotFound, "not_found", "gray_rule_not_found", "gray rule not found")
			return
		}
		afterWrite(c, a, "gray_rule.delete", map[string]any{"id": id}, "gray")
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"id": id, "deleted": true}})
	}
}
