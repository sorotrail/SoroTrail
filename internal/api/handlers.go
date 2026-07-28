// handleHealth returns the health status of the service.
// GET /health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
defer cancel()

// Check database health
dbHealthy := s.store.Ping(ctx) == nil

// If the store implements IsHealthy, use that for more accurate status
type healthChecker interface {
IsHealthy() bool
}
if hc, ok := s.store.(healthChecker); ok {
dbHealthy = hc.IsHealthy()
}

status := "ok"
httpStatus := http.StatusOK

if !dbHealthy {
status = "unhealthy"
httpStatus = http.StatusServiceUnavailable
}

// Also check RPC health
rpcHealthy := true
if err := s.rpc.Ping(ctx); err != nil {
rpcHealthy = false
}

resp := map[string]interface{}{
"status": status,
"checks": map[string]interface{}{
"database": map[string]interface{}{
"status": "ok",
},
"rpc": map[string]interface{}{
"status": "ok",
},
},
}

if !dbHealthy {
resp["checks"].(map[string]interface{})["database"].(map[string]interface{})["status"] = "unhealthy"
}
if !rpcHealthy {
resp["checks"].(map[string]interface{})["rpc"].(map[string]interface{})["status"] = "unhealthy"
}

w.Header().Set("Content-Type", "application/json")
w.WriteHeader(httpStatus)
json.NewEncoder(w).Encode(resp)
}