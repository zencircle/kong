local _M = {}
local _MT = { __index = _M, }

local utils = require "kong.tools.utils"

local type = type
local fmt = string.format
local tb_insert = table.insert
local null = ngx.null
local encode_base64 = ngx.encode_base64
local sha256 = utils.sha256_hex
local marshall = require("kong.db.declarative.marshaller").marshall
local unmarshall = require("kong.db.declarative.marshaller").unmarshall

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
          tb_insert(uniques, fname)
        end

      else
        tb_insert(uniques, fname)
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

      tb_insert(keys, key)
    end
  end

  return keys
end

local function gen_workspace_key(schema, entity)
  local keys = {}
  local entity_name = schema.name

  if not schema.workspaceable then
    tb_insert(keys, entity_name .. "||@list")
    return keys
  end

  local ws_id = get_ws_id(schema, entity)

  tb_insert(keys, entity_name .. "|" .. ws_id .. "|@list")
  tb_insert(keys, entity_name .. "|*|@list")

  return keys
end


local function gen_foreign_key(schema, entity)
  local foreign_fields = {}
  for fname, fdata in schema:each_field() do
    local is_foreign = fdata.type == "foreign"
    local fdata_reference = fdata.reference

    if is_foreign then
      foreign_fields[fname] = fdata_reference
    end
  end

  local entity_name = schema.name
  local ws_ids = { "*", get_ws_id(schema, entity) }

  local keys = {}
  for name, ref in pairs(foreign_fields) do
    ngx.log(ngx.ERR, "xxx name = ", name, " ref = ", ref)
    local fid = entity[name] and entity[name].id
    if not fid then
      goto continue
    end

    for _, ws_id in ipairs(ws_ids) do
      local key = entity_name .. "|" .. ws_id .. "|" .. ref .. "|" ..
                  fid .. "|@list"
      tb_insert(keys, key)
    end

    ::continue::
  end

  return keys
end

local function get_marshall_value(obj)
  local value = marshall(obj)
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
  local entity_name = schema.name

  if entity_name == "clustering_data_planes" then
    return true
  end

  local connector = kong.db.connector
  ngx.log(ngx.ERR, "xxx insert into cache_entries: ", entity_name)

  local stmt = "insert into cache_entries(revision, key, value) " ..
               "values(%d, '%s', decode('%s', 'base64')) " ..
               "ON CONFLICT (key) " ..
               "DO UPDATE " ..
               "  SET revision = EXCLUDED.revision, value = EXCLUDED.value" ..
               ";"

  local dao = kong.db[entity_name]

  local revision = get_revision()
  local cache_key = gen_cache_key(dao, schema, entity)
  ngx.log(ngx.ERR, "xxx cache_key = ", cache_key)

  local global_key = gen_global_cache_key(dao, entity)
  local schema_key = gen_schema_cache_key(dao, schema, entity)

  local value = get_marshall_value(entity)

  local sql = fmt(stmt, revision, cache_key, value)

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

  -- workspace key

  local ws_keys = gen_workspace_key(schema, entity)

  for _, key in ipairs(ws_keys) do
    local sel_stmt = "select value from cache_entries " ..
                 "where key='%s'"
    local sql = fmt(sel_stmt, key)
      ngx.log(ngx.ERR, "xxx sql = ", sql)
    res, err = connector:query(sql)
    if not res then
      ngx.log(ngx.ERR, "xxx err = ", err)
      return nil, err
    end
    local value = res and res[1] and res[1].value

    if value then
      local value = unmarshall(value)
      tb_insert(value, cache_key)
      value = get_marshall_value(value)
      ngx.log(ngx.ERR, "xxx upsert for ", key)
      res, err = connector:query(fmt(stmt, revision, key, value))
      --ngx.log(ngx.ERR, "xxx ws_key err = ", err)

    else

      ngx.log(ngx.ERR, "xxx no value for ", key)

      local value = get_marshall_value({cache_key})
      sql = fmt(stmt, revision, key, value)
      --ngx.log(ngx.ERR, "xxx sql:", sql)
      --ngx.log(ngx.ERR, "xxx cache_key :", cache_key)

      res, err = connector:query(sql)
      --ngx.log(ngx.ERR, "xxx ws_key err = ", err)
    end
  end

  -- foreign key
  --ngx.log(ngx.ERR, "xxx = ", require("inspect")(entity))
  local fkeys = gen_foreign_key(schema, entity)

  for _, key in ipairs(fkeys) do
    ngx.log(ngx.ERR, "xxx fkey = ", key)
    local sel_stmt = "select value from cache_entries " ..
                 "where key='%s'"
    local sql = fmt(sel_stmt, key)
      ngx.log(ngx.ERR, "xxx sql = ", sql)
    res, err = connector:query(sql)
    if not res then
      ngx.log(ngx.ERR, "xxx err = ", err)
      return nil, err
    end
    local value = res and res[1] and res[1].value

    if value then
      local value = unmarshall(value)
      tb_insert(value, cache_key)
      value = get_marshall_value(value)
      ngx.log(ngx.ERR, "xxx upsert for ", key)
      res, err = connector:query(fmt(stmt, revision, key, value))
      --ngx.log(ngx.ERR, "xxx ws_key err = ", err)

    else

      ngx.log(ngx.ERR, "xxx no value for ", key)

      local value = get_marshall_value({cache_key})
      sql = fmt(stmt, revision, key, value)
      --ngx.log(ngx.ERR, "xxx sql:", sql)
      --ngx.log(ngx.ERR, "xxx cache_key :", cache_key)

      res, err = connector:query(sql)
      --ngx.log(ngx.ERR, "xxx ws_key err = ", err)
    end
  end

  return true
end

return _M
