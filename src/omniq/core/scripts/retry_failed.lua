local anchor = KEYS[1]
local job_id = ARGV[1]
local now_ms = tonumber(ARGV[2] or "0")

local DEFAULT_GROUP_LIMIT = 1

local function derive_base(a)
  if a == nil or a == "" then return "" end
  if string.sub(a, -5) == ":meta" then
    return string.sub(a, 1, -6)
  end
  return a
end

local function to_i(v)
  if v == false or v == nil or v == '' then return 0 end
  local n = tonumber(v)
  if n == nil then return 0 end
  return math.floor(n)
end

local base = derive_base(anchor)

local k_job     = base .. ":job:" .. job_id
local k_wait    = base .. ":wait"
local k_active  = base .. ":active"
local k_delayed = base .. ":delayed"
local k_failed  = base .. ":failed"
local k_gready  = base .. ":groups:ready"

if redis.call("EXISTS", k_job) ~= 1 then
  return {"ERR", "NO_JOB"}
end

local st = redis.call("HGET", k_job, "state") or ""
if st ~= "failed" then
  return {"ERR", "NOT_FAILED"}
end

redis.call("ZREM", k_active, job_id)
redis.call("ZREM", k_delayed, job_id)
redis.call("LREM", k_wait, 0, job_id)
redis.call("LREM", k_failed, 0, job_id)

redis.call("HSET", k_job,
  "state", "wait",
  "attempt", "0",
  "updated_ms", tostring(now_ms),
  "lease_token", "",
  "lock_until_ms", "",
  "due_ms", ""
)

local gid = redis.call("HGET", k_job, "gid") or ""

if gid ~= "" then
  local k_gwait     = base .. ":g:" .. gid .. ":wait"
  local k_ginflight = base .. ":g:" .. gid .. ":inflight"
  local k_glimit    = base .. ":g:" .. gid .. ":limit"

  redis.call("RPUSH", k_gwait, job_id)

  local inflight = to_i(redis.call("GET", k_ginflight))
  local limit = to_i(redis.call("GET", k_glimit))
  if limit <= 0 then limit = DEFAULT_GROUP_LIMIT end

  if inflight < limit then
    redis.call("ZADD", k_gready, now_ms, gid)
  end
else
  redis.call("RPUSH", k_wait, job_id)
end

return {"OK"}
