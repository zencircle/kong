local _M = {}
--local _MT = { __index = _M, }

local utils = require "kong.tools.utils"
local constants = require "kong.constants"
--local lmdb = require "resty.lmdb"
local txn = require "resty.lmdb.transaction"

local type = type
local fmt = string.format
local tb_insert = table.insert
local null = ngx.null
local encode_base64 = ngx.encode_base64
local sha256 = utils.sha256_hex
local exiting = ngx.worker.exiting
local marshall = require("kong.db.declarative.marshaller").marshall
local unmarshall = require("kong.db.declarative.marshaller").unmarshall

local is_http_subsystem = ngx.config.subsystem == "http"

local DECLARATIVE_HASH_KEY = constants.DECLARATIVE_HASH_KEY

local current_version

local uniques = {}
local foreigns = {}

-- generate from schemas
local cascade_deleting_schemas = {
  upstreams = { "targets", },
  consumers = { "plugins", },
  routes = { "plugins", },
  services = { "plugins", },
}

-- 1e8ff358-fbba-4f32-ac9b-9f896c02b2d8
local function get_ws_id(schema, entity)
  if not schema.workspaceable then
    return ""
  end

  local ws_id = entity.ws_id

  if ws_id == null or ws_id == nil then
    ws_id = kong.default_workspace
    entity.ws_id = ws_id
  end

  return ws_id
end

-- upstreams:37add863-a3e4-4fcb-9784-bf1d43befdfa:::::1e8ff358-fbba-4f32-ac9b-9f896c02b2d8
local function gen_cache_key(dao, schema, entity)
  local ws_id = get_ws_id(schema, entity)

  local cache_key = dao:cache_key(entity.id, nil, nil, nil, nil, ws_id)

  return cache_key
end

-- upstreams:37add863-a3e4-4fcb-9784-bf1d43befdfa:::::*
local function gen_global_cache_key(dao, entity)
  local ws_id = "*"

  local cache_key = dao:cache_key(entity.id, nil, nil, nil, nil, ws_id)

  return cache_key
end

-- targets:37add863-a3e4-4fcb-9784-bf1d43befdfa:127.0.0.1:8081::::1e8ff358-fbba-4f32-ac9b-9f896c02b2d8
local function gen_schema_cache_key(dao, schema, entity)
  if not schema.cache_key then
    return nil
  end

  local cache_key = dao:cache_key(entity)

  return cache_key
end

-- upstreams|1e8ff358-fbba-4f32-ac9b-9f896c02b2d8|name:9aa44d94160d95b7ebeaa1e6540ffb68379a23cd4ee2f6a0ab7624a7b2dd6623
local function unique_field_key(schema_name, ws_id, field, value, unique_across_ws)
  if unique_across_ws then
    ws_id = ""
  end

  -- LMDB imposes a default limit of 511 for keys, but the length of our unique
  -- value might be unbounded, so we'll use a checksum instead of the raw value
  value = sha256(value)

  return schema_name .. "|" .. ws_id .. "|" .. field .. ":" .. value
end

-- may have many unique_keys
local function gen_unique_cache_key(schema, entity)
  local db = kong.db

  local unique_fields = uniques[schema.name]
  if not unique_fields then
    unique_fields = {}

    for fname, fdata in schema:each_field() do
      local is_foreign = fdata.type == "foreign"
      local fdata_reference = fdata.reference

      if fdata.unique then
        if is_foreign then
          if #db[fdata_reference].schema.primary_key == 1 then
            tb_insert(unique_fields, fname)
          end

        else
          tb_insert(unique_fields, fname)
        end
      end
    end -- for schema:each_field()

    uniques[schema.name] = unique_fields
  end

  local ws_id = get_ws_id(schema, entity)

  local keys = {}
  for i = 1, #unique_fields do
    local unique = unique_fields[i]
    local unique_key = entity[unique]
    if unique_key then
      if type(unique_key) == "table" then
        local _
        -- this assumes that foreign keys are not composite
        _, unique_key = next(unique_key)
      end

      local key = unique_field_key(schema.name, ws_id, unique, unique_key,
                                   schema.fields[unique].unique_across_ws)

      tb_insert(keys, key)
    end
  end

  return keys
end

-- upstreams|1e8ff358-fbba-4f32-ac9b-9f896c02b2d8|@list
-- upstreams|*|@list
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

-- targets|1e8ff358-fbba-4f32-ac9b-9f896c02b2d8|upstreams|37add863-a3e4-4fcb-9784-bf1d43befdfa|@list
-- targets|*|upstreams|37add863-a3e4-4fcb-9784-bf1d43befdfa|@list
local function gen_foreign_key(schema, entity)
  local foreign_fields = foreigns[schema.name]

  if not foreign_fields then
    foreign_fields = {}
    for fname, fdata in schema:each_field() do
      local is_foreign = fdata.type == "foreign"
      local fdata_reference = fdata.reference

      if is_foreign then
        foreign_fields[fname] = fdata_reference
      end
    end
    foreigns[schema.name] = foreign_fields
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

-- base64 for inserting into postgres
local function get_marshall_value(obj)
  local value = marshall(obj)
  --ngx.log(ngx.ERR, "xxx value size = ", #value)

  return encode_base64(value)
end

local function get_revision()
  local connector = kong.db.connector

  local sql = "select nextval('cache_revision');"

  local res, err = connector:query(sql)
  if not res then
  ngx.log(ngx.ERR, "xxx err = ", err)
    return nil, err
  end

  --ngx.log(ngx.ERR, "xxx revison = ", require("inspect")(res))
  --return tonumber(res[1].nextval)
  current_version = tonumber(res[1].nextval)

  return current_version
end


local upsert_stmt = "insert into cache_entries(revision, key, value) " ..
                    "values(%d, '%s', decode('%s', 'base64')) " ..
                    "ON CONFLICT (key) " ..
                    "DO UPDATE " ..
                    "  SET revision = EXCLUDED.revision, value = EXCLUDED.value"

local del_stmt = "delete from cache_entries " ..
                 "where key='%s'"

local insert_changs_stmt = "insert into cache_changes(revision, key, value, event) " ..
                           "values(%d, '%s', decode('%s', 'base64'), %d)"


-- key: routes|*|@list
-- result may be nil or empty table
local function query_list_value(connector, key)
  local sel_stmt = "select value from cache_entries " ..
                   "where key='%s'"
  local sql = fmt(sel_stmt, key)
    ngx.log(ngx.ERR, "xxx sql = ", sql)
  local res, err = connector:query(sql)
  if not res then
    ngx.log(ngx.ERR, "xxx err = ", err)
    return nil, err
  end

  local value = res and res[1] and res[1].value

  return value
end

local NIL_MARSHALL_VALUE = get_marshall_value("")

-- event: 0=>reserved, 1=>create, 2=>update 3=>delete
local function insert_into_changes(connector, revision, key, value, event)
  assert(type(key) == "string")

  -- nil => delete an entry
  if value == nil then
    value = NIL_MARSHALL_VALUE
  end

  local sql = fmt(insert_changs_stmt,
                  revision, key, value, event)
  local res, err = connector:query(sql)
  if not res then
    return nil, err
  end

  return true
end

-- targets|*|@list
-- targets|5c3275ba-8bc8-4def-86ba-8d79107cc002|@list
-- targets|*|upstreams|94c3a25d-01f3-4da1-be72-79a1715dd120|@list
-- targets|5c3275ba-8bc8-4def-86ba-8d79107cc002|upstreams|94c3a25d-01f3-4da1-be72-79a1715dd120|@list
local function upsert_list_value(connector, list_key, revision, cache_key)

  local value = query_list_value(connector, list_key)

  local res, err

  if value then
    local value = unmarshall(value)
    tb_insert(value, cache_key)
    value = get_marshall_value(value)
    ngx.log(ngx.ERR, "xxx upsert for ", list_key)
    res, err = connector:query(fmt(upsert_stmt, revision, list_key, value))
    --ngx.log(ngx.ERR, "xxx ws_key err = ", err)

    -- 2 => update existed data
    insert_into_changes(connector, revision, list_key, value, 2)

  else

    ngx.log(ngx.ERR, "xxx no value for ", list_key)

    local value = get_marshall_value({cache_key})
    local sql = fmt(upsert_stmt, revision, list_key, value)
    --ngx.log(ngx.ERR, "xxx sql:", sql)
    --ngx.log(ngx.ERR, "xxx cache_key :", cache_key)

    res, err = connector:query(sql)
    --ngx.log(ngx.ERR, "xxx ws_key err = ", err)

    -- 1 => create
    insert_into_changes(connector, revision, list_key, value, 1)
  end
end

-- ignore schema clustering_data_planes
function _M.upsert(schema, entity, old_entity)
  local entity_name = schema.name

  if entity_name == "clustering_data_planes" then
    return true
  end

  -- for cache_changes table
  local changed_keys = {}

  local connector = kong.db.connector
  ngx.log(ngx.ERR, "xxx insert into cache_entries: ", entity_name)

  local dao = kong.db[entity_name]

  local revision = get_revision()

  local cache_key = gen_cache_key(dao, schema, entity)
  local global_key = gen_global_cache_key(dao, entity)
  local schema_key = gen_schema_cache_key(dao, schema, entity)

  ngx.log(ngx.ERR, "xxx cache_key = ", cache_key)
  ngx.log(ngx.ERR, "xxx schema_key = ", schema_key)

  tb_insert(changed_keys, cache_key)
  tb_insert(changed_keys, global_key)

  if schema_key then
    tb_insert(changed_keys, schema_key)
  end

  local is_create = old_entity == nil

  local value = get_marshall_value(entity)

  local res, err

  for _, key in ipairs(changed_keys) do
    res, err = connector:query(fmt(upsert_stmt, revision, key, value))
    if not res then
      ngx.log(ngx.ERR, "xxx err = ", err)
      return nil, err
    end

    -- insert into cache_changes
    insert_into_changes(connector,
                        revision, key, value, is_create and 1 or 2)
  end

  local unique_keys = gen_unique_cache_key(schema, entity)
  for _, key in ipairs(unique_keys) do
    res, err = connector:query(fmt(upsert_stmt, revision, key, value))

    -- insert into cache_changes
    insert_into_changes(connector,
                        revision, key, value, is_create and 1 or 2)
  end

  local sql

  if is_create then

    -- workspace key

    local ws_keys = gen_workspace_key(schema, entity)

    for _, key in ipairs(ws_keys) do
      upsert_list_value(connector, key, revision, cache_key)
    end

    -- foreign key
    --ngx.log(ngx.ERR, "xxx = ", require("inspect")(entity))
    local fkeys = gen_foreign_key(schema, entity)

    for _, key in ipairs(fkeys) do
      upsert_list_value(connector, key, revision, cache_key)
    end

    return true
  end   -- is_create

  ngx.log(ngx.ERR, "xxx old entity.ws_id = ", old_entity.ws_id)

  -- update, remove old keys
  local old_schema_key = gen_schema_cache_key(dao, schema, old_entity)
  ngx.log(ngx.ERR, "xxx old_schema_key = ", old_schema_key)
  if old_schema_key ~= schema_key then
    sql = fmt(del_stmt, old_schema_key)
    res, err = connector:query(sql)

    -- 3 => delete
    insert_into_changes(connector, revision, old_schema_key, nil, 3)
  end

  local old_unique_keys = gen_unique_cache_key(schema, old_entity)

  for _, key in ipairs(old_unique_keys) do
    ngx.log(ngx.ERR, "xxx old unique key = ", key)
    local exist = false
    for _, k in ipairs(unique_keys) do
      if key == k then
        exist = true
        break
      end
    end

    -- find out old keys then delete them
    if not exist then
      sql = fmt(del_stmt, key)
      res, err = connector:query(sql)

      -- 3 => delete
      insert_into_changes(connector, revision, key, nil, 3)
    end
  end

  return true
end

function _M.delete(schema, entity)
  local entity_name = schema.name

  if entity_name == "clustering_data_planes" then
    return true
  end

  local connector = kong.db.connector
  ngx.log(ngx.ERR, "xxx delete from cache_entries: ", entity_name)

  local dao = kong.db[entity_name]

  local cache_key = gen_cache_key(dao, schema, entity)
  local global_key = gen_global_cache_key(dao, entity)
  local schema_key = gen_schema_cache_key(dao, schema, entity)

  local keys = gen_unique_cache_key(schema, entity)

  tb_insert(keys, cache_key)
  tb_insert(keys, global_key)
  if schema_key then
    tb_insert(keys, schema_key)
  end

  local revision = get_revision()

  local sql
  local res, err
  for _, key in ipairs(keys) do
    sql = fmt(del_stmt, key)
    ngx.log(ngx.ERR, "xxx delete sql = ", sql)

    res, err = connector:query(sql)
    if not res then
      ngx.log(ngx.ERR, "xxx err = ", err)
      return nil, err
    end

    -- 3 => delete
    insert_into_changes(connector, revision, key, nil, 3)
  end

  -- workspace key

  local ws_keys = gen_workspace_key(schema, entity)

  for _, key in ipairs(ws_keys) do
    local value = query_list_value(connector, key)

    if value then
      local list = unmarshall(value)
      --ngx.log(ngx.ERR, "xxx re-arrange list is: ", unpack(list))

      -- remove this cache_key
      local new_list = {}
      for _,v in ipairs(list) do
        if v ~= cache_key then
          tb_insert(new_list, v)
        end
      end
      value = get_marshall_value(new_list)
      ngx.log(ngx.ERR, "xxx delete for ", key)
      res, err = connector:query(fmt(upsert_stmt, revision, key, value))
      --ngx.log(ngx.ERR, "xxx ws_key err = ", err)

      -- 2 => update existed data
      insert_into_changes(connector, revision, key, value, 2)
    end
  end

  -- foreign key
  local fkeys = gen_foreign_key(schema, entity)

  for _, key in ipairs(fkeys) do
    local value = query_list_value(connector, key)

    if value then
      local list = unmarshall(value)
      --ngx.log(ngx.ERR, "xxx re-arrange list is: ", unpack(list))

      local new_list = {}
      for _,v in ipairs(list) do
        if v ~= cache_key then
          tb_insert(new_list, v)
        end
      end
      value = get_marshall_value(new_list)
      ngx.log(ngx.ERR, "xxx delete for ", key)
      res, err = connector:query(fmt(upsert_stmt, revision, key, value))
      --ngx.log(ngx.ERR, "xxx ws_key err = ", err)

      -- 2 => update existed data
      insert_into_changes(connector, revision, key, value, 2)
    end
  end

  -- cascade delete
  local cascade_deleting = cascade_deleting_schemas[entity_name]
  if not cascade_deleting then
    return true
  end

  -- here we only delete foreign keys
  -- dao will cascade delete other entities
  local ws_ids = { "*", get_ws_id(schema, entity) }

  for _, v in ipairs(cascade_deleting) do
    --local del_schema = kong.db[v].schema

    for _, ws_id in ipairs(ws_ids) do
      local fkey = v .. "|" .. ws_id .. "|" .. entity_name .. "|" ..
                   entity.id .. "|@list"
      sql = fmt(del_stmt, fkey)
      ngx.log(ngx.ERR, "xxx delete sql = ", sql)
      res, err = connector:query(sql)

      -- 3 => delete
      insert_into_changes(connector, revision, fkey, nil, 3)
    end

  end

  return true
end

local function begin_transaction(db)
  if db.strategy == "postgres" then
    local ok, err = db.connector:connect("read")
    if not ok then
      return nil, err
    end

    ok, err = db.connector:query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;", "read")
    if not ok then
      return nil, err
    end
  end

  return true
end


local function end_transaction(db)
  if db.strategy == "postgres" then
    -- just finish up the read-only transaction,
    -- either COMMIT or ROLLBACK is fine.
    db.connector:query("ROLLBACK;", "read")
    db.connector:setkeepalive()
  end
end


function _M.export_config(skip_ws, skip_disabled_entities)
  -- default skip_ws=false and skip_disabled_services=true
  if skip_ws == nil then
    skip_ws = false
  end

  if skip_disabled_entities == nil then
    skip_disabled_entities = true
  end

  -- TODO: disabled_services

  local db = kong.db

  local ok, err = begin_transaction(db)
  if not ok then
    return nil, err
  end

  local export_stmt = "select revision, key, value " ..
               "from cache_entries;"

  local res, err = db.connector:query(export_stmt)
  if not res then
    ngx.log(ngx.ERR, "xxx err = ", err)
    end_transaction(db)
    return nil, err
  end

  end_transaction(db)

  return res
end

function _M.export_inc_config(dp_revision)
  local db = kong.db

  local ok, err = begin_transaction(db)
  if not ok then
    return nil, err
  end

  local export_stmt = "select revision, key, value,event " ..
                      "from cache_changes " ..
                      "where revision > " .. dp_revision
  ngx.log(ngx.ERR, "xxx _M.export_inc_config = ", export_stmt)

  local res, err = db.connector:query(export_stmt)
  if not res then
    ngx.log(ngx.ERR, "xxx err = ", err)
    end_transaction(db)
    return nil, err
  end

  end_transaction(db)

  return res
end


local function load_into_cache(entries)
  ngx.log(ngx.ERR, "xxx count = ", #entries)

  --local is_incremental = entries[1].event ~= nil
  local is_full_sync = entries[1].event == nil

  local default_ws

  local t = txn.begin(#entries)

  -- full sync will drop all data
  if is_full_sync then
    t:db_drop(false)
  end

  local latest_revision = 0
  for _, entry in ipairs(entries) do
    latest_revision = math.max(latest_revision, entry.revision)
    ngx.log(ngx.ERR, "xxx revision = ", entry.revision, " key = ", entry.key)

    if entry.event and entry.event == 3 then
      -- incremental delete
      t:set(entry.key, nil)

    else
      t:set(entry.key, entry.value)
    end

    -- find the default workspace id
    if not default_ws then
      if entry.key == "workspaces:default:::::" then
        local obj = unmarshall(entry.value)
        default_ws = obj.id
        ngx.log(ngx.ERR, "xxx find default_ws = ", default_ws)
      end
    end
  end -- entries

  -- we can get current_version from lmdb
  t:set(DECLARATIVE_HASH_KEY, tostring(latest_revision))

  local ok, err = t:commit()
  if not ok then
    return nil, err
  end

  ngx.log(ngx.ERR, "xxx latest_revision = ", latest_revision)

  --current_version = latest_revision

  --kong.default_workspace = default_workspace

  kong.core_cache:purge()
  kong.cache:purge()

  if not default_ws then
    default_ws = kong.default_workspace
  end

  return true, nil, default_ws
end

local function load_into_cache_with_events_no_lock(entries)
  if exiting() then
    return nil, "exiting"
  end
  --ngx.log(ngx.ERR, "xxx load_into_cache_with_events_no_lock = ", #entries)

  local ok, err, default_ws = load_into_cache(entries)
  if not ok then
    if err:find("MDB_MAP_FULL", nil, true) then
      return nil, "map full"

    else
      return nil, err
    end
  end

  local worker_events = kong.worker_events

  --local default_ws = "6af4a340-fab2-4ed8-953d-21b852133fa6"

  local reconfigure_data = {
    default_ws,
    -- other hash is nil, trigger router/balancer rebuild
  }

  -- go to runloop/handler reconfigure_handler
  ok, err = worker_events.post("declarative", "reconfigure", reconfigure_data)
  if ok ~= "done" then
    return nil, "failed to broadcast reconfigure event: " .. (err or ok)
  end

  -- TODO: send to stream subsystem
  if is_http_subsystem and #kong.configuration.stream_listeners > 0 then
    -- update stream if necessary
    ngx.log(ngx.ERR, "xxx update stream if necessary = ")
  end


  if exiting() then
    return nil, "exiting"
  end

  return true
end

local DECLARATIVE_LOCK_TTL = 60
local DECLARATIVE_RETRY_TTL_MAX = 10
local DECLARATIVE_LOCK_KEY = "declarative:lock"

-- copied from declarative/init.lua
function _M.load_into_cache_with_events(entries)
  --ngx.log(ngx.ERR, "xxx load_into_cache_with_events = ", #entries)
  local kong_shm = ngx.shared.kong

  local ok, err = kong_shm:add(DECLARATIVE_LOCK_KEY, 0, DECLARATIVE_LOCK_TTL)
  if not ok then
    if err == "exists" then
      local ttl = math.min(kong_shm:ttl(DECLARATIVE_LOCK_KEY), DECLARATIVE_RETRY_TTL_MAX)
      return nil, "busy", ttl
    end

    kong_shm:delete(DECLARATIVE_LOCK_KEY)
    return nil, err
  end

  ok, err = load_into_cache_with_events_no_lock(entries)
  kong_shm:delete(DECLARATIVE_LOCK_KEY)

  return ok, err
end

function _M.get_current_version()
  if current_version then
    return current_version
  end

  local connector = kong.db.connector

  local sql = "SELECT last_value FROM cache_revision;"

  local res, err = connector:query(sql)
  if not res then
    ngx.log(ngx.ERR, "xxx err = ", err)
    return nil, err
  end

  ngx.log(ngx.ERR, "xxx revison = ", require("inspect")(res))
  --return tonumber(res[1].nextval)
  current_version = tonumber(res[1].last_value)

  return current_version
end

-- 1 => enable, 0 => disable
-- flag `SYNC_TEST` in clustering/control_plane.lua
_M.enable = 1

return _M
