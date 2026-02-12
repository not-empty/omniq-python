local anchor   = KEYS[1]
local child_id = ARGV[1]

local function derive_base(a)
  if a == nil or a == "" then return "" end
  if string.sub(a, -5) == ":meta" then
    return string.sub(a, 1, -6)
  end
  return a
end

local function to_i(v)
  if v == false or v == nil or v == "" then return 0 end
  local n = tonumber(v)
  if n == nil then return 0 end
  return math.floor(n)
end

local base = derive_base(anchor)

if child_id == nil or child_id == "" then
  return {"ERR", "CHILD_REQUIRED"}
end

local k_count = base .. ":count"
local k_done  = base .. ":done"

if redis.call("EXISTS", k_count) ~= 1 then
  return {"ERR", "NO_COUNTER"}
end

local added = redis.call("SADD", k_done, child_id)

if added == 1 then
  local remaining = to_i(redis.call("DECR", k_count))

  if remaining <= 0 then
    redis.call("DEL", k_count)
    redis.call("DEL", k_done)
    return {"OK", "0"}
  end

  return {"OK", tostring(remaining)}
end

local cur = to_i(redis.call("GET", k_count))
if cur <= 0 then
  return {"OK", "0"}
end

return {"OK", tostring(cur)}
