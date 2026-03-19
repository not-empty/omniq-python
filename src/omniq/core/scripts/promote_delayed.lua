local anchor      = KEYS[1]
local now_ms      = tonumber(ARGV[1] or "0")
local max_promote = tonumber(ARGV[2] or "1000")

local DEFAULT_GROUP_LIMIT = 1

local function derive_base(a)
  if a == nil or a == "" then return "" end
  if string.sub(a, -5) == ":meta" then
    return string.sub(a, 1, -6)
  end
  return a
end

local base = derive_base(anchor)

local k_delayed = base .. ":delayed"
local k_wait    = base .. ":wait"
local k_gready  = base .. ":groups:ready"

local k_stats  = base .. ":stats"
local k_queues = "omniq:queues"

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

local function group_limit_for(gid)
  local k_glimit = base .. ":g:" .. gid .. ":limit"
  local lim = to_i(redis.call("GET", k_glimit))
  if lim <= 0 then return DEFAULT_GROUP_LIMIT end
  return lim
end

local ids = redis.call("ZRANGEBYSCORE", k_delayed, "-inf", now_ms, "LIMIT", 0, max_promote)
local promoted = 0

local dec_delayed = 0
local inc_waiting = 0
local inc_group_waiting = 0
local inc_waiting_total = 0
local inc_groups_ready = 0

for i=1,#ids do
  local job_id = ids[i]
  if redis.call("ZREM", k_delayed, job_id) == 1 then
    local k_job = base .. ":job:" .. job_id
    redis.call("HSET", k_job, "state", "wait", "updated_ms", tostring(now_ms))

    dec_delayed = dec_delayed - 1

    local gid = redis.call("HGET", k_job, "gid")
    if gid and gid ~= "" then
      local k_gwait = base .. ":g:" .. gid .. ":wait"
      redis.call("RPUSH", k_gwait, job_id)

      inc_group_waiting = inc_group_waiting + 1
      inc_waiting_total = inc_waiting_total + 1

      local inflight = to_i(redis.call("GET", base .. ":g:" .. gid .. ":inflight"))
      local limit = group_limit_for(gid)
      if inflight < limit then
        local added = redis.call("ZADD", k_gready, "NX", now_ms, gid)
        if added == 1 then
          inc_groups_ready = inc_groups_ready + 1
        end
      end
    else
      redis.call("RPUSH", k_wait, job_id)

      inc_waiting = inc_waiting + 1
      inc_waiting_total = inc_waiting_total + 1
    end

    promoted = promoted + 1
  end
end

if promoted > 0 then
  redis.call("SADD", k_queues, base)

  if dec_delayed ~= 0 then
    hincrby_floor0(k_stats, "delayed", dec_delayed)
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

return {"OK", tostring(promoted)}