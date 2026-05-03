local anchor = KEYS[1]
local now_ms = tonumber(ARGV[1] or "0")
local count  = tonumber(ARGV[2] or "0")

local DEFAULT_GROUP_LIMIT = 1
local MAX_BATCH = 100

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

local function hincrby_floor0(key, field, delta)
  local v = to_i(redis.call("HINCRBY", key, field, delta))
  if v < 0 then
    redis.call("HSET", key, field, "0")
    return 0
  end
  return v
end

local base = derive_base(anchor)

local k_wait        = base .. ":wait"
local k_active      = base .. ":active"
local k_delayed     = base .. ":delayed"
local k_failed      = base .. ":failed"
local k_gready      = base .. ":groups:ready"
local k_stats       = base .. ":stats"
local k_idx_wait    = base .. ":idx:wait"
local k_idx_delayed = base .. ":idx:delayed"
local k_idx_failed  = base .. ":idx:failed"

local out = {}

local function push(job_id, status, reason)
  table.insert(out, job_id)
  table.insert(out, status)
  if status == "ERR" then
    table.insert(out, reason or "UNKNOWN")
  end
end

if count <= 0 then
  return out
end

if count > MAX_BATCH then
  return {"ERR", "BATCH_TOO_LARGE", tostring(MAX_BATCH)}
end

if #ARGV < (2 + count) then
  return {"ERR", "BAD_ARGS"}
end

local dec_failed = 0
local inc_waiting = 0
local inc_group_waiting = 0
local inc_waiting_total = 0
local inc_groups_ready = 0
local ok_count = 0

for i = 1, count do
  local job_id = ARGV[2 + i]
  if job_id == nil or job_id == "" then
    push("", "ERR", "BAD_JOB_ID")
  else
    local k_job = base .. ":job:" .. job_id

    if redis.call("EXISTS", k_job) ~= 1 then
      push(job_id, "ERR", "NO_JOB")
    else
      local st = redis.call("HGET", k_job, "state") or ""
      if st ~= "failed" then
        push(job_id, "ERR", "NOT_FAILED")
      else
        redis.call("ZREM", k_delayed, job_id)
        redis.call("LREM", k_wait, 0, job_id)
        redis.call("LREM", k_failed, 0, job_id)

        redis.call("ZREM", k_idx_delayed, job_id)
        redis.call("ZREM", k_idx_failed, job_id)

        redis.call("HSET", k_job,
          "state", "wait",
          "attempt", "0",
          "updated_ms", tostring(now_ms),
          "lease_token", "",
          "lock_until_ms", "",
          "due_ms", "",
          "failed_ms", ""
        )

        local gid = redis.call("HGET", k_job, "gid") or ""

        dec_failed = dec_failed - 1

        if gid ~= "" then
          local k_gwait     = base .. ":g:" .. gid .. ":wait"
          local k_ginflight = base .. ":g:" .. gid .. ":inflight"
          local k_glimit    = base .. ":g:" .. gid .. ":limit"

          redis.call("RPUSH", k_gwait, job_id)
          redis.call("ZADD", k_idx_wait, now_ms, job_id)

          inc_group_waiting = inc_group_waiting + 1
          inc_waiting_total = inc_waiting_total + 1

          local inflight = to_i(redis.call("GET", k_ginflight))
          local limit = to_i(redis.call("GET", k_glimit))
          if limit <= 0 then limit = DEFAULT_GROUP_LIMIT end

          if inflight < limit then
            local added = redis.call("ZADD", k_gready, "NX", now_ms, gid)
            if added == 1 then
              inc_groups_ready = inc_groups_ready + 1
            end
          end
        else
          redis.call("RPUSH", k_wait, job_id)
          redis.call("ZADD", k_idx_wait, now_ms, job_id)
          inc_waiting = inc_waiting + 1
          inc_waiting_total = inc_waiting_total + 1
        end

        ok_count = ok_count + 1
        push(job_id, "OK", nil)
      end
    end
  end
end

if ok_count > 0 then
  if dec_failed ~= 0 then
    hincrby_floor0(k_stats, "failed", dec_failed)
  end

  if inc_waiting ~= 0 then
    redis.call("HINCRBY", k_stats, "waiting", inc_waiting)
  end

  if inc_group_waiting ~= 0 then
    redis.call("HINCRBY", k_stats, "group_waiting", inc_group_waiting)
  end

  if inc_waiting_total ~= 0 then
    redis.call("HINCRBY", k_stats, "waiting_total", inc_waiting_total)
  end

  if inc_groups_ready ~= 0 then
    redis.call("HINCRBY", k_stats, "groups_ready", inc_groups_ready)
  end

  redis.call("HSET", k_stats, "last_activity_ms", tostring(now_ms))
end

return out
