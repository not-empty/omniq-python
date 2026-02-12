local anchor = KEYS[1]
local lane   = ARGV[1] or ""
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

local function dec_floor0(key)
  local v = to_i(redis.call("DECR", key))
  if v < 0 then
    redis.call("SET", key, "0")
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

local k_active    = base .. ":active"
local k_wait      = base .. ":wait"
local k_delayed   = base .. ":delayed"
local k_failed    = base .. ":failed"
local k_completed = base .. ":completed"
local k_gready    = base .. ":groups:ready"

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
              redis.call("DEL", k_job)
              push(job_id, "OK", nil)
            end

          elseif lane == "delayed" then
            if redis.call("ZSCORE", k_delayed, job_id) == false then
              push(job_id, "ERR", "NOT_IN_LANE")
            else
              redis.call("ZREM", k_delayed, job_id)
              redis.call("DEL", k_job)
              push(job_id, "OK", nil)
            end

          elseif lane == "failed" then
            removed = redis.call("LREM", k_failed, 1, job_id)
            if removed <= 0 then
              push(job_id, "ERR", "NOT_IN_LANE")
            else
              redis.call("DEL", k_job)
              push(job_id, "OK", nil)
            end

          elseif lane == "completed" then
            removed = redis.call("LREM", k_completed, 1, job_id)
            if removed <= 0 then
              push(job_id, "ERR", "NOT_IN_LANE")
            else
              redis.call("DEL", k_job)
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
                redis.call("ZADD", k_gready, 0, gid)
              else
                redis.call("ZREM", k_gready, gid)
              end

              redis.call("DEL", k_job)
              push(job_id, "OK", nil)
            end
          end
        end
      end
    end
  end
end

return out
