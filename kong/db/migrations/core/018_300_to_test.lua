local sha256 = require("kong.tools.utils").sha256_hex
local marshall = require("kong.db.declarative.marshaller").marshall
local encode_base64 = ngx.encode_base64

-- add default workspace
local function p_generate_cache_entries(connector)
  for ws, err in connector:iterate("select * from workspaces") do
    if err then
      return nil, err
    end
    --ngx.log(ngx.ERR, "xxx ws.name:", ws.name)
    --ngx.log(ngx.ERR, "xxx ws.id:", ws.id)

    local keys = {
      cache_key = "workspaces:" .. ws.id .. ":::::",
      global_key =  "workspaces:" .. ws.id .. ":::::*",
      schema_key = "workspaces:" .. ws.name .. ":::::",
      unique_key = "workspaces||name:" .. sha256(ws.name),
      --ws_key = "workspaces||@list",
    }

    local revision = 1
    local value = encode_base64(marshall(ws))
    local stmt = "insert into cache_entries(revision, key, value) " ..
                 "values(%d, '%s', decode('%s', 'base64'))"

    for name, key in pairs(keys) do
      --ngx.log(ngx.ERR, "xxx ", name, " : ", key)

      local sql = string.format(stmt, revision, key, value)

      local _, err = connector:query(sql)
      if err then
        return nil, err
      end
    end
  end   -- for

  return true
end

return {
    postgres = {
      up = [[
        CREATE TABLE IF NOT EXISTS "cache_entries" (
            "revision"   bigint   NOT NULL,
            "key"        TEXT     UNIQUE NOT NULL,
            "value"      BYTEA    NULL
            );

        CREATE SEQUENCE "cache_revision";

        CREATE TABLE IF NOT EXISTS "cache_changes" (
            "revision"   bigint   NOT NULL,
            "key"        TEXT     NOT NULL,
            "value"      BYTEA    NOT NULL,
            "event"      smallint NOT NULL
            );
      ]],

      up_f = p_generate_cache_entries,
    },
    cassandra = {
    },
  }
