local anchor       = KEYS[1]

local job_id       = ARGV[1]
local payload      = ARGV[2] or ""
local max_attempts = tonumber(ARGV[3] or "1")
local timeout_ms   = tonumber(ARGV[4] or "60000")
local backoff_ms   = tonumber(ARGV[5] or "5000")
local now_ms       = tonumber(ARGV[6] or "0")
local due_ms       = tonumber(ARGV[7] or "0")
local gid          = ARGV[8]
local group_limit  = tonumber(ARGV[9] or "0")

local DEFAULT_GROUP_LIMIT = 1

local function derive_base(a)
  if a == nil or a == "" then return "" end
  if string.sub(a, -5) == ":meta" then
    return string.sub(a, 1, -6)
  end
  return a
end

local base = derive_base(anchor)

local k_job        = base .. ":job:" .. job_id
local k_delayed    = base .. ":delayed"
local k_wait       = base .. ":wait"
local k_has_groups = base .. ":has_groups"

local is_grouped = (gid ~= nil and gid ~= "")

if is_grouped then
  redis.call("HSET", k_job,
    "id", job_id,
    "payload", payload,
    "gid", gid,
    "state", "wait",
    "attempt", "0",
    "max_attempts", tostring(max_attempts),
    "timeout_ms", tostring(timeout_ms),
    "backoff_ms", tostring(backoff_ms),
    "created_ms", tostring(now_ms),
    "updated_ms", tostring(now_ms)
  )

  redis.call("SET", k_has_groups, "1")

  local k_glimit = base .. ":g:" .. gid .. ":limit"
  if group_limit ~= nil and group_limit > 0 then
    if redis.call("EXISTS", k_glimit) == 0 then
      redis.call("SET", k_glimit, tostring(group_limit))
    end
  end
else
  redis.call("HSET", k_job,
    "id", job_id,
    "payload", payload,
    "state", "wait",
    "attempt", "0",
    "max_attempts", tostring(max_attempts),
    "timeout_ms", tostring(timeout_ms),
    "backoff_ms", tostring(backoff_ms),
    "created_ms", tostring(now_ms),
    "updated_ms", tostring(now_ms)
  )
end

if due_ms ~= nil and due_ms > now_ms then
  redis.call("ZADD", k_delayed, due_ms, job_id)
  redis.call("HSET", k_job, "state", "delayed", "due_ms", tostring(due_ms))
else
  if is_grouped then
    local k_gwait = base .. ":g:" .. gid .. ":wait"
    redis.call("RPUSH", k_gwait, job_id)

    local k_ginflight = base .. ":g:" .. gid .. ":inflight"
    local inflight = tonumber(redis.call("GET", k_ginflight) or "0")

    local limit = tonumber(redis.call("GET", base .. ":g:" .. gid .. ":limit") or tostring(DEFAULT_GROUP_LIMIT))
    if inflight < limit then
      local k_gready = base .. ":groups:ready"
      redis.call("ZADD", k_gready, now_ms, gid)
    end
  else
    redis.call("RPUSH", k_wait, job_id)
  end
end

return {"OK", job_id}
