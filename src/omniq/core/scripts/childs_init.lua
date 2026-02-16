local anchor   = KEYS[1]
local expected = ARGV[1]

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

if expected == nil or expected == "" then
  return {"ERR", "NO_EXPECTED"}
end

local n = to_i(expected)
if n <= 0 then
  return {"ERR", "BAD_EXPECTED"}
end

local k_count = base .. ":count"
local k_done  = base .. ":done"

if redis.call("EXISTS", k_count) == 1 then
  return {"ERR", "ALREADY_INIT"}
end

redis.call("SET", k_count, tostring(n))
redis.call("DEL", k_done)

return {"OK"}
