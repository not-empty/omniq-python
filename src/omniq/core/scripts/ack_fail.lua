local anchor      = KEYS[1]
local job_id      = ARGV[1]
local now_ms      = tonumber(ARGV[2] or "0")
local lease_token = ARGV[3]
local err_msg     = ARGV[4]

local DEFAULT_GROUP_LIMIT = 1
local MAX_ERR_BYTES = 4096

local function derive_base(a)
  if a == nil or a == "" then return "" end
  if string.sub(a, -5) == ":meta" then
    return string.sub(a, 1, -6)
  end
  return a
end

local base = derive_base(anchor)

local k_job     = base .. ":job:" .. job_id
local k_active  = base .. ":active"
local k_delayed = base .. ":delayed"
local k_failed  = base .. ":failed"
local k_gready  = base .. ":groups:ready"

local function to_i(v)
  if v == false or v == nil or v == '' then return 0 end
  local n = tonumber(v)
  if n == nil then return 0 end
  return math.floor(n)
end

local function dec_floor0(key)
  local v = to_i(redis.call("DECR", key))
  if v < 0 then
    redis.call("SET", key, "0")
    return 0
  end
  return v
end

local function group_limit_for(gid)
  local k_glimit = base .. ":g:" .. gid .. ":limit"
  local lim = to_i(redis.call("GET", k_glimit))
  if lim <= 0 then return DEFAULT_GROUP_LIMIT end
  return lim
end

local function maybe_store_last_error()
  if err_msg == nil or err_msg == "" then return end
  if string.len(err_msg) > MAX_ERR_BYTES then
    err_msg = string.sub(err_msg, 1, MAX_ERR_BYTES)
  end
  redis.call("HSET", k_job,
    "last_error", err_msg,
    "last_error_ms", tostring(now_ms)
  )
end

if lease_token == nil or lease_token == "" then
  return {"ERR", "TOKEN_REQUIRED"}
end

local cur_token = redis.call("HGET", k_job, "lease_token") or ""
if cur_token ~= lease_token then
  return {"ERR", "TOKEN_MISMATCH"}
end

if redis.call("ZREM", k_active, job_id) ~= 1 then
  return {"ERR", "NOT_ACTIVE"}
end

maybe_store_last_error()

local gid = redis.call("HGET", k_job, "gid")
if gid and gid ~= "" then
  local k_ginflight = base .. ":g:" .. gid .. ":inflight"
  local inflight = dec_floor0(k_ginflight)
  local limit = group_limit_for(gid)
  local k_gwait = base .. ":g:" .. gid .. ":wait"
  if inflight < limit and to_i(redis.call("LLEN", k_gwait)) > 0 then
    redis.call("ZADD", k_gready, now_ms, gid)
  end
end

local attempt      = to_i(redis.call("HGET", k_job, "attempt"))
local max_attempts = to_i(redis.call("HGET", k_job, "max_attempts"))
if max_attempts <= 0 then max_attempts = 1 end
local backoff_ms   = to_i(redis.call("HGET", k_job, "backoff_ms"))

if attempt >= max_attempts then
  redis.call("HSET", k_job,
    "state", "failed",
    "updated_ms", tostring(now_ms),
    "lease_token", "",
    "lock_until_ms", ""
  )
  redis.call("LPUSH", k_failed, job_id)
  return {"FAILED"}
end

local due_ms = now_ms + backoff_ms
redis.call("HSET", k_job,
  "state", "delayed",
  "due_ms", tostring(due_ms),
  "updated_ms", tostring(now_ms),
  "lease_token", "",
  "lock_until_ms", ""
)
redis.call("ZADD", k_delayed, due_ms, job_id)

return {"RETRY", tostring(due_ms)}
