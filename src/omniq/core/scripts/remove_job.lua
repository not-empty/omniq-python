local anchor = KEYS[1]
local job_id = ARGV[1]
local lane   = ARGV[2] or ""

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

local base = derive_base(anchor)

local k_job       = base .. ":job:" .. job_id
local k_wait      = base .. ":wait"
local k_active    = base .. ":active"
local k_delayed   = base .. ":delayed"
local k_failed    = base .. ":failed"
local k_completed = base .. ":completed"
local k_gready    = base .. ":groups:ready"
local k_stats  = base .. ":stats"
local k_queues = "omniq:queues"

if redis.call("EXISTS", k_job) ~= 1 then
  return {"ERR", "NO_JOB"}
end

if redis.call("ZSCORE", k_active, job_id) ~= false then
  return {"ERR", "ACTIVE"}
end

local st  = redis.call("HGET", k_job, "state") or ""
local gid = redis.call("HGET", k_job, "gid") or ""

local expected = ""
if lane == "wait" then expected = "wait"
elseif lane == "delayed" then expected = "delayed"
elseif lane == "failed" then expected = "failed"
elseif lane == "completed" then expected = "completed"
elseif lane == "gwait" then expected = "wait"
else
  return {"ERR", "LANE_MISMATCH"}
end

if st ~= expected then
  return {"ERR", "LANE_MISMATCH"}
end

if lane == "gwait" and (gid == nil or gid == "") then
  return {"ERR", "LANE_MISMATCH"}
end

local ginflight_dec = 0
if gid ~= "" then
  local lt = redis.call("HGET", k_job, "lease_token") or ""
  local lu = redis.call("HGET", k_job, "lock_until_ms") or ""
  local looks_reserved = (lt ~= "") or (lu ~= "")
  if looks_reserved then
    local k_ginflight = base .. ":g:" .. gid .. ":inflight"
    dec_floor0(k_ginflight)
    ginflight_dec = 1
  end
end

local removed = 0
local groups_ready_delta = 0

if lane == "wait" then
  removed = redis.call("LREM", k_wait, 1, job_id)

elseif lane == "delayed" then
  if redis.call("ZSCORE", k_delayed, job_id) == false then
    return {"ERR", "NOT_IN_LANE"}
  end
  removed = redis.call("ZREM", k_delayed, job_id)

elseif lane == "failed" then
  removed = redis.call("LREM", k_failed, 1, job_id)

elseif lane == "completed" then
  removed = redis.call("LREM", k_completed, 1, job_id)

elseif lane == "gwait" then
  local k_gwait = base .. ":g:" .. gid .. ":wait"

  local was_ready = (redis.call("ZSCORE", k_gready, gid) ~= false)

  removed = redis.call("LREM", k_gwait, 1, job_id)

  local inflight = to_i(redis.call("GET", base .. ":g:" .. gid .. ":inflight"))
  local limit = group_limit_for(base, gid)
  local qlen = to_i(redis.call("LLEN", k_gwait))

  local should_be_ready = (qlen > 0 and inflight < limit)

  if should_be_ready then
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
end

if (lane == "wait" or lane == "failed" or lane == "completed" or lane == "gwait") and removed <= 0 then
  return {"ERR", "NOT_IN_LANE"}
end

redis.call("DEL", k_job)

redis.call("SADD", k_queues, base)

if lane == "wait" then
  hincrby_floor0(k_stats, "waiting", -1)
  hincrby_floor0(k_stats, "waiting_total", -1)

elseif lane == "delayed" then
  hincrby_floor0(k_stats, "delayed", -1)

elseif lane == "failed" then
  hincrby_floor0(k_stats, "failed", -1)

elseif lane == "completed" then
  hincrby_floor0(k_stats, "completed_kept", -1)

elseif lane == "gwait" then
  hincrby_floor0(k_stats, "group_waiting", -1)
  hincrby_floor0(k_stats, "waiting_total", -1)

  if groups_ready_delta ~= 0 then
    hincrby_floor0(k_stats, "groups_ready", groups_ready_delta)
  end
end

redis.call("HSET", k_stats, "last_activity_ms", tostring(redis.call("TIME")[1] * 1000))

return {"OK"}