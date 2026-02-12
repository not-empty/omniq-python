local anchor = KEYS[1]

local function derive_base(a)
  if a == nil or a == "" then return "" end
  if string.sub(a, -5) == ":meta" then
    return string.sub(a, 1, -6)
  end
  return a
end

local base = derive_base(anchor)

local k_paused = base .. ":paused"

local removed = redis.call("DEL", k_paused)

return removed
