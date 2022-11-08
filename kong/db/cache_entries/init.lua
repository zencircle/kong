local _M = {}
local _MT = { __index = _M, }

local utils = require "kong.tools.utils"

local type = type
local fmt = string.format
local insert = table.insert
local null = ngx.null
local encode_base64 = ngx.encode_base64
local sha256 = utils.sha256_hex
local marshall = require("kong.db.declarative.marshaller").marshall

local function get_ws_id(schema, entity)
  local ws_id = ""
  if schema.workspaceable then
    local entity_ws_id = entity.ws_id
    if entity_ws_id == null or entity_ws_id == nil then
      entity_ws_id = kong.default_workspace
    end
    entity.ws_id = entity_ws_id
    ws_id = entity_ws_id
  end

  return ws_id
end

local function gen_cache_key(dao, schema, entity)
  local ws_id = get_ws_id(schema, entity)

  local cache_key = dao:cache_key(entity.id, nil, nil, nil, nil, ws_id)

  return cache_key
end

local function gen_global_cache_key(dao, entity)
  local ws_id = "*"

  local cache_key = dao:cache_key(entity.id, nil, nil, nil, nil, ws_id)

  return cache_key
end

local function gen_schema_cache_key(dao, schema, entity)
  if not schema.cache_key then
    return nil
  end

  local cache_key = dao:cache_key(entity)

  return cache_key
end

local function unique_field_key(schema_name, ws_id, field, value, unique_across_ws)
  if unique_across_ws then
    ws_id = ""
  end

  -- LMDB imposes a default limit of 511 for keys, but the length of our unique
  -- value might be unbounded, so we'll use a checksum instead of the raw value
  value = sha256(value)

  return schema_name .. "|" .. ws_id .. "|" .. field .. ":" .. value
end

local function gen_unique_cache_key(schema, entity)
  local db = kong.db
  local uniques = {}

  for fname, fdata in schema:each_field() do
    local is_foreign = fdata.type == "foreign"
    local fdata_reference = fdata.reference

    if fdata.unique then
      if is_foreign then
        if #db[fdata_reference].schema.primary_key == 1 then
          insert(uniques, fname)
        end

      else
        insert(uniques, fname)
      end
    end
  end

  local keys = {}
  for i = 1, #uniques do
    local unique = uniques[i]
    local unique_key = entity[unique]
    if unique_key then
      if type(unique_key) == "table" then
        local _
        -- this assumes that foreign keys are not composite
        _, unique_key = next(unique_key)
      end

      local key = unique_field_key(schema.name, entity.ws_id or "", unique, unique_key,
                                   schema.fields[unique].unique_across_ws)

      table.insert(keys, key)
    end
  end

  return keys
end

local function gen_global_workspace_key()
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

local function get_revision()
  local connector = kong.db.connector

  local sql = "select nextval('cache_revision');"

  local res, err = connector:query(sql)
  if not res then
  ngx.log(ngx.ERR, "xxx err = ", err)
    return nil, err
  end

  --ngx.log(ngx.ERR, "xxx revison = ", require("inspect")(res))
  return tonumber(res[1].nextval)
end

function _M.insert(schema, entity)
  local connector = kong.db.connector
  ngx.log(ngx.ERR, "xxx insert into cache_entries")

  local stmt = "insert into cache_entries(revision, key, value) " ..
               "values(%d, '%s', decode('%s', 'base64'))"

  local dao = kong.db[schema.name]

  local revision = get_revision()
  local key = gen_cache_key(dao, schema, entity)
  ngx.log(ngx.ERR, "xxx key = ", key)

  local global_key = gen_global_cache_key(dao, entity)
  local schema_key = gen_schema_cache_key(dao, schema, entity)

  local value = get_marshall_value(entity)

  local sql = fmt(stmt, revision, key, value)

  local res, err = connector:query(sql)

  if not res then
  ngx.log(ngx.ERR, "xxx err = ", err)

    return nil, err
  end

  res, err = connector:query(fmt(stmt, revision, global_key, value))

  if schema_key then
    res, err = connector:query(fmt(stmt, revision, schema_key, value))
  end

  local unique_keys = gen_unique_cache_key(schema, entity)
  for _, key in ipairs(unique_keys) do
    res, err = connector:query(fmt(stmt, revision, key, value))
  end

  return true
end

return _M
