local anchor = KEYS[1]
local lane   = ARGV[1] or ""
local count  = tonumber(ARGV[2] or "0")
local now_ms = tonumber(ARGV[3 + count] or "0")

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

local function group_limit_for(base, gid)
  local lim = to_i(redis.call("GET", base .. ":g:" .. gid .. ":limit"))
  if lim <= 0 then return DEFAULT_GROUP_LIMIT end
  return lim
end

local function expected_state_for_lane(l)
  if l == "wait" then return "wait" end
  if l == "delayed" then return "delayed" end
  if l == "failed" then return "failed" end
  if l == "completed" then return "completed" end
  if l == "gwait" then return "wait" end
  return ""
end

local base = derive_base(anchor)

local k_active        = base .. ":active"
local k_wait          = base .. ":wait"
local k_delayed       = base .. ":delayed"
local k_failed        = base .. ":failed"
local k_completed     = base .. ":completed"
local k_gready        = base .. ":groups:ready"
local k_stats         = base .. ":stats"
local k_queues        = "omniq:queues"
local k_idx_wait      = base .. ":idx:wait"
local k_idx_active    = base .. ":idx:active"
local k_idx_delayed   = base .. ":idx:delayed"
local k_idx_failed    = base .. ":idx:failed"
local k_idx_completed = base .. ":idx:completed"

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

local expected_state = expected_state_for_lane(lane)
if expected_state == "" then
  return {"ERR", "BAD_LANE"}
end

if #ARGV < (2 + count) then
  return {"ERR", "BAD_ARGS"}
end

local dec_waiting = 0
local dec_group_waiting = 0
local dec_waiting_total = 0
local dec_delayed = 0
local dec_failed = 0
local dec_completed_kept = 0
local groups_ready_delta = 0
local removed_ok = 0

for i = 1, count do
  local job_id = ARGV[2 + i]
  if job_id == nil or job_id == "" then
    push("", "ERR", "BAD_JOB_ID")
  else
    local k_job = base .. ":job:" .. job_id

    if redis.call("EXISTS", k_job) ~= 1 then
      push(job_id, "ERR", "NO_JOB")
    else
      if redis.call("ZSCORE", k_active, job_id) ~= false then
        push(job_id, "ERR", "ACTIVE")
      else
        local st  = redis.call("HGET", k_job, "state") or ""
        local gid = redis.call("HGET", k_job, "gid") or ""

        if st ~= expected_state then
          push(job_id, "ERR", "LANE_MISMATCH")
        elseif lane == "gwait" and (gid == nil or gid == "") then
          push(job_id, "ERR", "LANE_MISMATCH")
        else
          if gid ~= "" then
            local lt = redis.call("HGET", k_job, "lease_token") or ""
            local lu = redis.call("HGET", k_job, "lock_until_ms") or ""
            if (lt ~= "") or (lu ~= "") then
              dec_floor0(base .. ":g:" .. gid .. ":inflight")
            end
          end

          local removed = 0

          if lane == "wait" then
            removed = redis.call("LREM", k_wait, 1, job_id)
            if removed <= 0 then
              push(job_id, "ERR", "NOT_IN_LANE")
            else
              redis.call("ZREM", k_idx_wait, job_id)

              redis.call("DEL", k_job)
              dec_waiting = dec_waiting - 1
              dec_waiting_total = dec_waiting_total - 1
              removed_ok = removed_ok + 1
              push(job_id, "OK", nil)
            end

          elseif lane == "delayed" then
            if redis.call("ZSCORE", k_delayed, job_id) == false then
              push(job_id, "ERR", "NOT_IN_LANE")
            else
              removed = redis.call("ZREM", k_delayed, job_id)
              if removed <= 0 then
                push(job_id, "ERR", "NOT_IN_LANE")
              else
                redis.call("ZREM", k_idx_delayed, job_id)

                redis.call("DEL", k_job)
                dec_delayed = dec_delayed - 1
                removed_ok = removed_ok + 1
                push(job_id, "OK", nil)
              end
            end

          elseif lane == "failed" then
            removed = redis.call("LREM", k_failed, 1, job_id)
            if removed <= 0 then
              push(job_id, "ERR", "NOT_IN_LANE")
            else
              redis.call("ZREM", k_idx_failed, job_id)

              redis.call("DEL", k_job)
              dec_failed = dec_failed - 1
              removed_ok = removed_ok + 1
              push(job_id, "OK", nil)
            end

          elseif lane == "completed" then
            removed = redis.call("LREM", k_completed, 1, job_id)
            if removed <= 0 then
              push(job_id, "ERR", "NOT_IN_LANE")
            else
              redis.call("ZREM", k_idx_completed, job_id)

              redis.call("DEL", k_job)
              dec_completed_kept = dec_completed_kept - 1
              removed_ok = removed_ok + 1
              push(job_id, "OK", nil)
            end

          elseif lane == "gwait" then
            local k_gwait = base .. ":g:" .. gid .. ":wait"
            removed = redis.call("LREM", k_gwait, 1, job_id)
            if removed <= 0 then
              push(job_id, "ERR", "NOT_IN_LANE")
            else
              local inflight = to_i(redis.call("GET", base .. ":g:" .. gid .. ":inflight"))
              local limit = group_limit_for(base, gid)
              local qlen = to_i(redis.call("LLEN", k_gwait))

              if qlen > 0 and inflight < limit then
                local added = redis.call("ZADD", k_gready, "NX", 0, gid)
                if added == 1 then
                  groups_ready_delta = groups_ready_delta + 1
                end
              else
                local removed_ready = redis.call("ZREM", k_gready, gid)
                if removed_ready == 1 then
                  groups_ready_delta = groups_ready_delta - 1
                end
              end

              redis.call("ZREM", k_idx_wait, job_id)

              redis.call("DEL", k_job)
              dec_group_waiting = dec_group_waiting - 1
              dec_waiting_total = dec_waiting_total - 1
              removed_ok = removed_ok + 1
              push(job_id, "OK", nil)
            end
          end
        end
      end
    end
  end
end

if removed_ok > 0 then
  redis.call("SADD", k_queues, base)

  if dec_waiting ~= 0 then
    hincrby_floor0(k_stats, "waiting", dec_waiting)
  end

  if dec_group_waiting ~= 0 then
    hincrby_floor0(k_stats, "group_waiting", dec_group_waiting)
  end

  if dec_waiting_total ~= 0 then
    hincrby_floor0(k_stats, "waiting_total", dec_waiting_total)
  end

  if dec_delayed ~= 0 then
    hincrby_floor0(k_stats, "delayed", dec_delayed)
  end

  if dec_failed ~= 0 then
    hincrby_floor0(k_stats, "failed", dec_failed)
  end

  if dec_completed_kept ~= 0 then
    hincrby_floor0(k_stats, "completed_kept", dec_completed_kept)
  end

  if groups_ready_delta ~= 0 then
    hincrby_floor0(k_stats, "groups_ready", groups_ready_delta)
  end

  redis.call("HSET", k_stats, "last_activity_ms", tostring(now_ms))
end

return out