local _M = {}
local _MT = { __index = _M, }


local fmt = string.format
local encode_base64 = ngx.encode_base64

--function _M.new()
--  local self = {
--    db = kong.db,
--
--  }
--  return setmetatable(self, _MT)
--end

function _M.insert(entity)
  local connector = kong.db.connector

  local stmt = "insert into cache_entries(revision, key, value) " ..
               "values(%d, %s, decode('%s', 'base64'))"

  local res, err = connector:query(fmt(stmt, 1, 'a', encode_base64('123')))

  if not res then
    return nil, err
  end

  return true
end

return _M
