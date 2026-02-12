local anchor      = KEYS[1]
local job_id      = ARGV[1]
local now_ms      = tonumber(ARGV[2] or "0")
local lease_token = ARGV[3]

local function derive_base(a)
  if a == nil or a == "" then return "" end
  if string.sub(a, -5) == ":meta" then
    return string.sub(a, 1, -6)
  end
  return a
end

local base = derive_base(anchor)

local k_job    = base .. ":job:" .. job_id
local k_active = base .. ":active"

local function to_i(v)
  if v == false or v == nil or v == '' then return 0 end
  local n = tonumber(v)
  if n == nil then return 0 end
  return math.floor(n)
end

if lease_token == nil or lease_token == "" then
  return {"ERR", "TOKEN_REQUIRED"}
end

local cur_token = redis.call("HGET", k_job, "lease_token") or ""
if cur_token ~= lease_token then
  return {"ERR", "TOKEN_MISMATCH"}
end

local cur_score = redis.call("ZSCORE", k_active, job_id)
if not cur_score then
  return {"ERR", "NOT_ACTIVE"}
end

local cur_lock_until = tonumber(cur_score) or 0

local timeout_ms = to_i(redis.call("HGET", k_job, "timeout_ms"))
if timeout_ms <= 0 then timeout_ms = 60000 end

local base_ms = cur_lock_until
if now_ms > base_ms then
  base_ms = now_ms
end

local lock_until = base_ms + timeout_ms

redis.call("HSET", k_job,
  "lock_until_ms", tostring(lock_until),
  "updated_ms", tostring(now_ms)
)
redis.call("ZADD", k_active, lock_until, job_id)

return {"OK", tostring(lock_until)}
