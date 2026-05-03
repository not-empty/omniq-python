local anchor   = KEYS[1]
local now_ms   = tonumber(ARGV[1] or "0")
local max_reap = tonumber(ARGV[2] or "1000")

local DEFAULT_GROUP_LIMIT = 1

local function derive_base(a)
  if a == nil or a == "" then return "" end
  if string.sub(a, -5) == ":meta" then
    return string.sub(a, 1, -6)
  end
  return a
end

local base = derive_base(anchor)

local k_active      = base .. ":active"
local k_delayed     = base .. ":delayed"
local k_failed      = base .. ":failed"
local k_gready      = base .. ":groups:ready"
local k_stats       = base .. ":stats"
local k_idx_active  = base .. ":idx:active"
local k_idx_delayed = base .. ":idx:delayed"
local k_idx_failed  = base .. ":idx:failed"

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

local function hincrby_floor0(key, field, delta)
  local v = to_i(redis.call("HINCRBY", key, field, delta))
  if v < 0 then
    redis.call("HSET", key, field, "0")
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

local ids = redis.call("ZRANGEBYSCORE", k_active, "-inf", now_ms, "LIMIT", 0, max_reap)
local reaped = 0

local dec_active = 0
local inc_delayed = 0
local inc_failed = 0
local inc_groups_ready = 0

for i=1,#ids do
  local job_id = ids[i]

  local score = redis.call("ZSCORE", k_active, job_id)
  if score and tonumber(score) and tonumber(score) > now_ms then
  else
    if redis.call("ZREM", k_active, job_id) == 1 then
      dec_active = dec_active - 1

      local k_job = base .. ":job:" .. job_id

      if redis.call("EXISTS", k_job) == 0 then
        redis.call("ZREM", k_idx_active, job_id)
        reaped = reaped + 1
      else
        redis.call("HSET", k_job, "lease_token", "")

        local gid = redis.call("HGET", k_job, "gid")
        if gid and gid ~= "" then
          local k_ginflight = base .. ":g:" .. gid .. ":inflight"
          local inflight = dec_floor0(k_ginflight)
          local limit = group_limit_for(gid)
          local k_gwait = base .. ":g:" .. gid .. ":wait"
          if inflight < limit and to_i(redis.call("LLEN", k_gwait)) > 0 then
            local added = redis.call("ZADD", k_gready, "NX", now_ms, gid)
            if added == 1 then
              inc_groups_ready = inc_groups_ready + 1
            end
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
            "failed_ms", tostring(now_ms),
            "lease_token", "",
            "lock_until_ms", ""
          )
          redis.call("ZREM", k_idx_active, job_id)
          redis.call("ZADD", k_idx_failed, now_ms, job_id)

          redis.call("LPUSH", k_failed, job_id)
          inc_failed = inc_failed + 1
        else
          local due_ms = now_ms + backoff_ms
          redis.call("HSET", k_job,
            "state", "delayed",
            "due_ms", tostring(due_ms),
            "updated_ms", tostring(now_ms),
            "lease_token", "",
            "lock_until_ms", ""
          )
          redis.call("ZREM", k_idx_active, job_id)
          redis.call("ZADD", k_idx_delayed, now_ms, job_id)

          redis.call("ZADD", k_delayed, due_ms, job_id)
          inc_delayed = inc_delayed + 1
        end

        reaped = reaped + 1
      end
    end
  end
end

if reaped > 0 then
  if dec_active ~= 0 then
    hincrby_floor0(k_stats, "active", dec_active)
  end

  if inc_delayed ~= 0 then
    redis.call("HINCRBY", k_stats, "delayed", inc_delayed)
  end

  if inc_failed ~= 0 then
    redis.call("HINCRBY", k_stats, "failed", inc_failed)
  end

  if inc_groups_ready ~= 0 then
    redis.call("HINCRBY", k_stats, "groups_ready", inc_groups_ready)
  end

  redis.call("HSET", k_stats, "last_activity_ms", tostring(now_ms))
end

return {"OK", tostring(reaped)}
