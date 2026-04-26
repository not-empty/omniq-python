local anchor = KEYS[1]
local now_ms = tonumber(ARGV[1] or "0")

local function derive_base(a)
  if a == nil or a == "" then return "" end
  if string.sub(a, -5) == ":meta" then
    return string.sub(a, 1, -6)
  end
  return a
end

local base = derive_base(anchor)

local k_paused = base .. ":paused"
if redis.call("EXISTS", k_paused) == 1 then
  return {"PAUSED"}
end

local DEFAULT_GROUP_LIMIT = 1
local MAX_GROUP_POPS = 10

local k_wait       = base .. ":wait"
local k_active     = base .. ":active"
local k_gready     = base .. ":groups:ready"
local k_rr         = base .. ":lane:rr"
local k_token_seq  = base .. ":lease:seq"
local k_stats      = base .. ":stats"
local k_queues     = "omniq:queues"
local k_idx_wait   = base .. ":idx:wait"
local k_idx_active = base .. ":idx:active"

local function to_i(v)
  if v == false or v == nil or v == '' then return 0 end
  local n = tonumber(v)
  if n == nil then return 0 end
  return math.floor(n)
end

local function hincrby_floor0(key, field, delta)
  local v = to_i(redis.call("HINCRBY", key, field, delta))
  if v < 0 then
    redis.call("HSET", key, field, "0")
    return 0
  end
  return v
end

local function new_lease_token(job_id)
  local seq = redis.call("INCR", k_token_seq)
  return redis.sha1hex(job_id .. ":" .. tostring(now_ms) .. ":" .. tostring(seq))
end

local function lease_job(job_id)
  local k_job = base .. ":job:" .. job_id

  local timeout_ms = to_i(redis.call("HGET", k_job, "timeout_ms"))
  if timeout_ms <= 0 then timeout_ms = 60000 end

  local attempt = to_i(redis.call("HGET", k_job, "attempt")) + 1
  local max_attempts = to_i(redis.call("HGET", k_job, "max_attempts"))
  local lock_until = now_ms + timeout_ms

  local payload = redis.call("HGET", k_job, "payload") or ""
  local gid = redis.call("HGET", k_job, "gid") or ""
  local first_started_ms = redis.call("HGET", k_job, "first_started_ms") or ""

  local lease_token = new_lease_token(job_id)

  if first_started_ms == "" then
    redis.call("HSET", k_job,
      "first_started_ms", tostring(now_ms)
    )
  end

  redis.call("HSET", k_job,
    "state", "active",
    "attempt", tostring(attempt),
    "lock_until_ms", tostring(lock_until),
    "lease_token", lease_token,
    "updated_ms", tostring(now_ms),
    "last_started_ms", tostring(now_ms)
  )

  redis.call("ZREM", k_idx_wait, job_id)
  redis.call("ZADD", k_idx_active, now_ms, job_id)

  redis.call("ZADD", k_active, lock_until, job_id)

  redis.call("HINCRBY", k_stats, "active", 1)
  redis.call("HSET", k_stats,
    "last_activity_ms", tostring(now_ms),
    "last_reserve_ms", tostring(now_ms)
  )

  return {"JOB", job_id, payload, tostring(lock_until), tostring(attempt), tostring(max_attempts), gid, lease_token}
end

local function try_ungrouped()
  local job_id = redis.call("LPOP", k_wait)
  if not job_id then
    return nil
  end

  hincrby_floor0(k_stats, "waiting", -1)
  hincrby_floor0(k_stats, "waiting_total", -1)

  return lease_job(job_id)
end

local function group_limit_for(gid)
  local k_glimit = base .. ":g:" .. gid .. ":limit"
  local lim = to_i(redis.call("GET", k_glimit))
  if lim <= 0 then return DEFAULT_GROUP_LIMIT end
  return lim
end

local function try_grouped()
  for _ = 1, MAX_GROUP_POPS do
    local popped = redis.call("ZPOPMIN", k_gready, 1)
    if not popped or #popped == 0 then
      return nil
    end

    local gid = popped[1]
    if not gid or gid == "" then
    else
      hincrby_floor0(k_stats, "groups_ready", -1)

      local k_gwait     = base .. ":g:" .. gid .. ":wait"
      local k_ginflight = base .. ":g:" .. gid .. ":inflight"

      local inflight = to_i(redis.call("GET", k_ginflight))
      local limit = group_limit_for(gid)

      if inflight >= limit then
        local added = redis.call("ZADD", k_gready, "NX", now_ms + 1, gid)
        if added == 1 then
          redis.call("HINCRBY", k_stats, "groups_ready", 1)
        end
      else
        local job_id = redis.call("LPOP", k_gwait)
        if not job_id then
        else
          inflight = to_i(redis.call("INCR", k_ginflight))

          hincrby_floor0(k_stats, "group_waiting", -1)
          hincrby_floor0(k_stats, "waiting_total", -1)

          if inflight < limit and to_i(redis.call("LLEN", k_gwait)) > 0 then
            local added = redis.call("ZADD", k_gready, "NX", now_ms, gid)
            if added == 1 then
              redis.call("HINCRBY", k_stats, "groups_ready", 1)
            end
          end

          return lease_job(job_id)
        end
      end
    end
  end

  return nil
end

redis.call("SADD", k_queues, base)

local rr = to_i(redis.call("GET", k_rr))

local res
if rr == 0 then
  res = try_grouped()
  if not res then res = try_ungrouped() end
else
  res = try_ungrouped()
  if not res then res = try_grouped() end
end

if not res then
  return {"EMPTY"}
end

if rr == 0 then
  redis.call("SET", k_rr, "1")
else
  redis.call("SET", k_rr, "0")
end

return res
