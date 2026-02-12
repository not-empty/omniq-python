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

local k_wait     = base .. ":wait"
local k_active   = base .. ":active"
local k_gready   = base .. ":groups:ready"
local k_rr       = base .. ":lane:rr"

local k_token_seq = base .. ":lease:seq"

local function to_i(v)
  if v == false or v == nil or v == '' then return 0 end
  local n = tonumber(v)
  if n == nil then return 0 end
  return math.floor(n)
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
  local lock_until = now_ms + timeout_ms

  local payload = redis.call("HGET", k_job, "payload") or ""
  local gid = redis.call("HGET", k_job, "gid") or ""

  local lease_token = new_lease_token(job_id)

  redis.call("HSET", k_job,
    "state", "active",
    "attempt", tostring(attempt),
    "lock_until_ms", tostring(lock_until),
    "lease_token", lease_token,
    "updated_ms", tostring(now_ms)
  )

  redis.call("ZADD", k_active, lock_until, job_id)

  return {"JOB", job_id, payload, tostring(lock_until), tostring(attempt), gid, lease_token}
end

local function try_ungrouped()
  local job_id = redis.call("LPOP", k_wait)
  if not job_id then
    return nil
  end
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
      local k_gwait     = base .. ":g:" .. gid .. ":wait"
      local k_ginflight = base .. ":g:" .. gid .. ":inflight"

      local inflight = to_i(redis.call("GET", k_ginflight))
      local limit = group_limit_for(gid)

      if inflight >= limit then
        redis.call("ZADD", k_gready, now_ms + 1, gid)
      else
        local job_id = redis.call("LPOP", k_gwait)
        if not job_id then
        else
          inflight = to_i(redis.call("INCR", k_ginflight))

          if inflight < limit and to_i(redis.call("LLEN", k_gwait)) > 0 then
            redis.call("ZADD", k_gready, now_ms, gid)
          end

          return lease_job(job_id)
        end
      end
    end
  end

  return nil
end

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
