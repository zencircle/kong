local _M = {}
local _MT = { __index = _M, }


local fmt = string.format
local encode_base64 = ngx.encode_base64
local marshall = require("kong.db.declarative.marshaller").marshall

local function gen_cache_key(dao, entity)
  local ws_id = ""

  local cache_key = dao:cache_key(entity.id, nil, nil, nil, nil, ws_id)

  return cache_key
end

local function gen_global_cache_key(dao, entity)
  local ws_id = "*"

  local cache_key = dao:cache_key(entity.id, nil, nil, nil, nil, ws_id)

  return cache_key
end

local function get_marshall_value(entity)
  local value = marshall(entity)
  ngx.log(ngx.ERR, "xxx value size = ", #value)

  return encode_base64(value)
end

--function _M.new()
--  local self = {
--    db = kong.db,
--
--  }
--  return setmetatable(self, _MT)
--end

function _M.insert(schema, entity)
  local connector = kong.db.connector
  ngx.log(ngx.ERR, "xxx insert into cache_entries")

  local stmt = "insert into cache_entries(revision, key, value) " ..
               "values(%d, '%s', decode('%s', 'base64'))"

  local dao = kong.db[schema.name]

  local revision = 1
  local key = gen_cache_key(dao, entity)
  ngx.log(ngx.ERR, "xxx key = ", key)

  local global_key = gen_global_cache_key(dao, entity)

  local value = get_marshall_value(entity)

  local sql = fmt(stmt, revision, key, value)

  local res, err = connector:query(sql)

  if not res then
  ngx.log(ngx.ERR, "xxx err = ", err)

    return nil, err
  end

  res, err = connector:query(fmt(stmt, revision, global_key, value))

  return true
end

return _M
