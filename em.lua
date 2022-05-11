-- Copyright (c) 2022 nya.works <bug@nya.works>
-- 
-- Permission to use, copy, modify, and distribute this software for any
-- purpose with or without fee is hereby granted, provided that the above
-- copyright notice and this permission notice appear in all copies.
-- 
-- THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
-- WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
-- MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
-- ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
-- WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
-- ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
-- OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

------------------
-- Module Setup --
------------------

-- required dependencies
local sqlite3 = require("lsqlite3")

-- optional dependencies
local jsonlib = nil

pcall(function()
	jsonlib = require("json")
end)

-- module
local em = {}

-- version
em.version = { 0, 3, 0 }
em.version_string = table.concat(em.version, ".")

-- registers
em.default_key = nil
em.on_change = nil
em.retry = false

-- flags
em.json = not not jsonlib

-- module variables
local entities = {}
local transaction = nil
local check_statement = nil
local pending_changes = false

-- entity metatable
local entity = {}
local entity_mt = { __index = entity }

-- lua compatibility
local unpack = unpack or table.unpack or error("No unpack()")

-----------------------
-- Utility functions --
-----------------------

-- Enquote something for safer SQL
local function quote(name)
	return "\""..name.."\""
end

local function step(statement)
	local code

	local attempts = 0
	local retry = em.retry
	local done = false
	
	repeat
		attempts = attempts + 1
		code = statement:step()

		if transaction or code ~= sqlite3.BUSY then
			done = true
		elseif type(retry) == "number" and attempts >= retry then
			done = true
		elseif (type(retry) == "function" or type(retry) == "table") and retry(attempts) then
			done = true
		else
			done = retry
		end

	until done

	return code
end

-- Execute a statement that only has zero or one result
local function execute(statement, f)
	local code, rv

	if f then
		code = step(statement)

		if code == sqlite3.DONE then
			statement:reset()
			return nil
		elseif code ~= sqlite3.ROW then
			statement:reset()
			error("Unexpected step code "..code)
		end

		rv = f(statement)
	end

	code = step(statement)

	if code ~= sqlite3.DONE then
		statement:reset()
		error("Unexpected step code "..code)
	end

	statement:reset()

	return rv
end

-- Execute a statement that only has zero or more results
local function execute_multi(statement, f)
	local code, results

	code = step(statement)

	results = {}

	f = f or statement.get_values

	while code == sqlite3.ROW do
		table.insert(results, f(statement))

		code = statement:step()
	end

	if code == sqlite3.DONE then
		statement:reset()
		return results
	else
		statement:reset()
		error("Unexpected step code "..code)
	end
end

local function get_first(statement)
	local values = statement:get_values()

	return values and values[1]
end

-- confirm that a sqlite3 call worked
local function confirm(code, error_msg, acceptable)
	acceptable = acceptable or { [sqlite3.OK] = true }

	if acceptable[code] then
		return
	end

	if error_msg == nil then
		error_msg = "Sqlite error"
	end

	error(error_msg.." (#"..code..")", 2)
end
em.confirm = confirm

local function table_remove(t, e)
	local idx = 1

	while t[idx] ~=  nil and t[idx] ~= e do
		idx = idx + 1
	end

	if t[idx] then
		table.remove(t, idx)
	end
end

local function mark_dirty(entity, row)
	entity.dirty[row] = true
	if not pending_changes then
		pending_changes = true

		if em.on_change then
			em.on_change()
		end
	end
end

-- creates a cache
local cache_mt = { __mode = "v" }
local function cache()
	return setmetatable({}, cache_mt)
end

-------------------
-- Field classes --
-------------------

local function class(default_options)
	return function(name, options)
		local result = {}

		if options == nil and name ~= nil and (type(name) == "table" or not name:match("[a-zA-Z]")) then
			options = name
			name = nil
		end

		if default_options then
			for k,v in pairs(default_options) do
				result[k] = v
			end
		end

		if name then
			result.name = name
		end

		if options then
			if type(options) == "string" then
				local optstr = options
				if result.class == "ID" then
					options = {
						required = optstr:match("!") and true or nil,
					}
				else
					options = {
						required = not optstr:match("?"),
						unique = optstr:match("!") and true or nil,
					}
				end

				if result.class =="ENTITY" and optstr:match("*") then
					result.virtual = true
				end
			end

			for k,v in pairs(options) do
				result[k] = v
			end
		end

		return result
	end
end

-- Standard classes
local classes = {
	text    = class{class="TEXT", required=true},
	numeric = class{class="NUMERIC", required=true},
	int     = class{class="INT", required=true},
	real    = class{class="REAL", required=true},
	blob    = class{class="BLOB", required=true},
	id      = class{class="ID", sqltype="INTEGER", unique=true},
}

-- Optional json class
if jsonlib then
	classes.json = class{class="JSON", sqltype="TEXT", required=true}
end

-- verbose
em.class = classes

-- shortcut
em.c = classes

-- Entity classes (foreign keys), can accept name or table object
local function entity_class(entity, ...)
	if type(entity) == "table" then
		entity = entity.name
	end

	return class{class="ENTITY", entity=entity, name=entity, required=true }(...)
end
em.fkey = entity_class

local bad_types = {
	["table"] = true,
	["function"] = true,
	["userdata"] = true,
	["thread"] = true,
}

local function make_transform(f)
	return function(field, value)
		if value == nil then
			if field.required then
				error("Field "..field.name.." is required but was set to nil.")
			end
			return nil, nil
		end

		if bad_types[type(value)] then
			error("Field "..field.name.." cannot be set to a "..type(value)..".")
		end

		local processed = f(value)
		if processed == nil then
			error("Field "..field.name.." cannot be set to value "..tostring(value)..".")
		end

		return processed, processed
	end
end

local transforms = {
	TEXT = make_transform(tostring),
	NUMERIC = make_transform(tonumber),
	INT = make_transform(function (value)
		return math.floor(tonumber(value))
	end),
	ENTITY = function(field, value)
		if value == nil then
			if field.required then
				error("Field "..field.name.." is required but was set to nil.")
			end
			return nil, nil
		end

		local _value = value
		if type(value) == "table" then
			local ventity = value.entity
			if field.entity ~= ventity.name then
				error("Field "..entity.name.."."..key.." cannot store entity "..entity.name)
			end

			local pkey = ventity.key
			_value = value[pkey]
		end

		if type(value) == "table" and value.rowid then
			return _value, _value
		else
			return value, _value
		end
	end,
}
transforms.BLOB = transforms.TEXT
transforms.REAL = transforms.NUMERIC
transforms.ID = transforms.INT

if jsonlib then
	transforms.JSON = function(field, value, entity, row)
		local json = nil
		local obj  = nil

		local json_mt = {}

		local new_table = function(source)
			local storage = {}
			local result = setmetatable({__storage = storage}, json_mt)

			for k,v in pairs(source) do
				if type(v) == "table" then
					v = new_table(v)
				end

				storage[k] = v
			end

			return result
		end

		json_mt.__index = function(self, key)
			return self.__storage[key]
		end

		json_mt.__newindex = function(self, key, value)
			if type(value) == "table" and getmetatable(value) ~= json_mt then
				value = new_table(value)
			end

			if type(value) == "function" or type(value) == "thread" or type(value) == "userdata" then
				error("cannot store "..type(value).." in json objects")
			end

			json = nil

			if entity and row then
				mark_dirty(entity, row)
			end

			self.__storage[key] = value
		end

		if value == nil then
			if field.required then
				error("Field "..field.name.." is required but was set to nil.")
			end
			return nil, nil
		end

		local unwind

		unwind = function(table)
			local result = {}

			for k,v in pairs(table.__storage) do
				if type(v) == "table" then
					v = unwind(v)
				end
				result[k] = v
			end

			return result
		end

		if type(value) == "string" then
			json = value
		elseif type(value) == "table" then
			obj = new_table(value)
		end

		local f = function(parsed)
			if parsed then
				if obj == nil then
					obj = new_table(jsonlib.decode(json))
				end
				return obj
			else
				if json == nil then
					json = jsonlib.encode(unwind(obj))
				end
				return json
			end
		end

		return f, f(false)
	end
end

local function transform_value(field, value, entity, row)
	return transforms[field.class](field, value, entity, row)
end


----------------------
-- module functions --
----------------------

-- close the db
function em.close()
	if em.db ~= nil then
		em.db:close()
		em.db = nil

		check_statement = nil
	end
end

function em.test(what, ...)
	return check_statement[what](check_statement, ...)
end

local function prepare(lines)
	local sql = table.concat(lines, " ")
	local statement = nil

	return function(option)
		if option == "sql" then
			return sql
		elseif option ~= nil then
			error("Expected nil or 'sql'")
		end

		if statement and not statement:isopen() then
			statement = nil
		end

		if statement == nil then
			if em.db == nil then
				error("Database is closed")
			end

			local code
			statement, code = em.db:prepare(sql)

			if statement == nil then
				error("Failed to prepare statement (code "..code.."): "..sql)
			end
		end

		return statement
	end
end

-- global prepared statements
check_statement = prepare{"SELECT count(1) FROM \"sqlite_master\" WHERE type='table' AND name=?"}

local function prepare_statements(entity)
	local name = entity.name

	name = quote(name)

	local field_names = {}
	local field_params = {}
	for i,v in ipairs(entity.field_names) do
		field_names[i] = quote(v)
		field_params[i] = "?"
	end

	local unique_checks = {}
	for i,v in ipairs(entity.unique_fields) do
		unique_checks[i] = quote(v).." = ?"
	end

	local key = quote(entity.key)

	local statements = {
		insert = prepare{
			"INSERT INTO "..name,
			"("..table.concat(field_names, ",")..")",
			"VALUES ("..table.concat(field_params, ", ")..")",
		},
		update = prepare{
			"UPDATE "..name,
			"SET ("..table.concat(field_names, ",")..")",
			"= ("..table.concat(field_params, ", ")..")",
			"WHERE rowid = ?",
		},
		delete = prepare{
			"DELETE FROM "..name.." WHERE rowid = ?",
		},
		get = prepare{
			"SELECT "..table.concat(field_names, ",")..",\"rowid\"",
			"FROM "..name,
			"WHERE "..key.." = ?",
		},
	}

	if #unique_checks > 0 then
		statements.is_unique = prepare{
			"SELECT EXISTS(SELECT 1 FROM "..name.." WHERE "..table.concat(unique_checks, " OR ")..")",
		}
		
		local reuse = #unique_checks == 1 and statements.is_unique

		statements.has = {}
		for i,v in ipairs(entity.unique_fields) do
			statements.has[v] = reuse or prepare{
				"SELECT EXISTS(SELECT 1 FROM "..name.." WHERE "..quote(v).." = ?)",
			}
		end

		statements.exists = statements.has[entity.key]
	end

	entity.statements = statements
end

-- open the db
function em.open(filename)
	em.close()
	if filename then
		em.db = sqlite3.open(filename)
	else
		em.db = sqlite3.open_memory()
	end
end

local function get_vfkey(parent, vfkey)
	if vfkey.get then
		return vfkey.get
	end

	local child = entities[vfkey.entity]

	if child == nil then
		return nil
	end

	local pname = parent.name
	local fkey = vfkey.key

	if fkey ~= nil then
		fkey = child.fields[fkey]
		if fkey == nil then
			error("Parent table "..parent.name.."."..vfkey.name.." refers to nonexistent child field "..child.name.."."..vfkey.key)
		end
	else
		for name,field in pairs(child.fields) do
			if field.class == "ENTITY" and field.entity == pname and not field.virtual then
				if fkey == nil then
					fkey = field
				else
					error("Parent table "..parent.name.."."..vfkey.name.." refers ambiguously to child table "..child.name.." which has multiple fkeys to parent")
				end
			end
		end

		if fkey == nil then
			error("Parent table "..parent.name.."."..vfkey.name.." refers to non-child "..child.name)
		end
	end

	if fkey.unique then
		if vfkey.multi == true then
			error("Parent table "..parent.name.."."..vfkey.name.." refers to child "..child.name.."."..fkey.name.." (expected multi, got singular)")
		end
		vfkey.multi = false
	else
		if vfkey.multi == false then
			error("Parent table "..parent.name.."."..vfkey.name.." refers to child "..child.name.."."..fkey.name.." (expected singular, got multi)")
		end
		vfkey.multi = true
	end

	local fkey_name = fkey.name

	local where = child:query({fkey_name, "=", ":key"})

	vfkey.get = function(pkey)
		if vfkey.multi then
			return where{key=pkey}

		else
			local result = child.caches[fkey_name][pkey]
			if result == nil then
				result = where{key=pkey}[1]
			end
			return result
		end
	end

	return vfkey.get
end

-- create a new table
function em.new(entity_name, key, fields, options)
	if entities[entity_name] then
		error("Table "..entity_name.." already exists")
	end

	local self = setmetatable({}, entity_mt)

	-- initial parsing
	local parsed = {}
	local field_names = {}

	if type(key) == "function" then
		key = key()
	end

	if type(key) == "table" then
		if key.name == nil then
			if type(em.default_key) == "string" then
				key.name = em.default_key
			else
				error("Table "..entity_name.." key is missing a name")
			end
		end

		parsed[key.name] = key

		table.insert(field_names, key.name)

		key = key.name
	end

	-- allow key-only tables
	fields = fields or {}

	if fields[1] then
		-- ordered fields
		for i,field in ipairs(fields) do
			local name = string.lower(field.name)
			if name == nil then
				error("Table "..entity_name.." field #"..i.." is missing a name")
			end

			parsed[name] = field
			table.insert(field_names, name)
		end
	else
		-- unordered fields - pkey goes first
		if key ~= nil and key ~= "rowid" then
			field_names[1] = key
		end

		for name,field in pairs(fields) do
			name = string.lower(name)

			parsed[name] = field

			if name ~= key then
				table.insert(field_names, name)
			end
		end
	end

	fields = parsed
	if #field_names == 0 then
		error("Table "..entity_name.." has no fields")
	end

	-- broad verifications
	if fields.rowid then
		error("Cannot overwrite rowid field")
	end

	if key == nil then
		key = "rowid"
	elseif key ~= "rowid" and fields[key] == nil then
		error("Table "..entity_name.." is missing key field "..key)
	end

	-- parse fields
	local unique_fields = {}
	local dependencies = {}
	local queue = {}

	for i,name in ipairs(field_names) do
		local field = fields[name]

		if type(name) ~= "string" then
			error("Fields must be strings")
		end

		-- string fields for convenience
		if type(field) == "string" then
			local tag, flags = field:match("^(.-)([?!*]*)$")

			local class = classes[tag]

			if class == nil then
				class = entity_class(tag, flags)
			else
				class = class(flags)
			end

			field = class
		elseif type(field) == "function" then
			field = field()
		elseif type(field) == "table" and getmetatable(field) == entity_mt then
			field = entity_class(field, { required = true })
		elseif type(field) ~= "table" or field.class == nil then
			error("Invalid field type for "..entity_name.."."..name)
		end

		field.name = name

		fields[name] = field

		if name == key then
			field.unique = true
		end

		if field.unique then
			table.insert(unique_fields, name)
		end

		if field.class == "ENTITY" and field.required then
			table.insert(queue, field.entity)
		end

		if field.class == "ID" then
			if key ~= name then
				error("ID can only be used for primary keys")
			end
		end
	end

	for name,field in pairs(fields) do
		if field.virtual then
			table_remove(field_names, name)
			if field.unique then
				table_remove(unique_fields, name)
			end
			field.required = false
		end
	end

	while #queue > 0 do
		local name = table.remove(queue)
		if name == entity_name then
			error("Circular dependency between "..entity_name.." and itself")
		end
		local entity = entities[name]
		if entity ~= nil and not dependencies[entity] then
			dependencies[entity] = true

			if entity.fields == nil then
				error("Bad table "..name.." from "..entity_name)
			end

			for _,field in pairs(entity.fields) do
				if field.class == "ENTITY" and field.required then
					if field.entity == entity_name then
						error("Circular dependency between "..entity_name.." and "..entity.name)
					end
					table.insert(queue, field.entity)
				end
			end
		end
	end

	local caches = {}
	for i,name in ipairs(unique_fields) do
		caches[name] = cache()
	end

	self.name = entity_name
	self.key = key
	self.fields = fields
	self.field_names = field_names
	self.unique_fields = unique_fields
	self.cache = caches[key]
	self.rows = cache()
	self.caches = caches
	self.keep = {}
	self.dirty = {}

	prepare_statements(self)

	entities[entity_name] = self

	return self
end

-- get an existing table
function em.get(name)
	return entities[name]
end

-- pairs() function for entities
function em.entities()
	return function(_, prev)
		return next(entities, prev)
	end, nil, nil
end

-- begin a transaction
function em.begin(strict)
	if transaction then
		if strict then
			error("Already began a transaction")
		end

		transaction.level = transaction.level + 1
		return
	end

	confirm(em.db:exec("BEGIN TRANSACTION"), "Failed to begin transaction")

	transaction = {
		level = 1,
		update = {},
	}
end

-- commit a transaction
function em.commit(force)
	if not transaction then
		error("No transaction")
	end

	if not force then
		local level = transaction.level - 1

		if level > 0 then
			transaction.level = level
			return
		end
	end

	confirm(em.db:exec("COMMIT TRANSACTION"), "Failed to commit transaction")

	for hook in pairs(transaction.update) do
		hook(true)
	end

	transaction = nil
end

-- flush all changed to the db
function em.raw_flush()
	local next_flush = entities
	local to_flush
	local total = -1
	local skip_fkeys = true -- gets inverted for first iteration
	local prev

	repeat
		to_flush = next_flush
		next_flush = {}
		prev = total
		total = 0
		skip_fkeys = not skip_fkeys

		for n,entity in pairs(to_flush) do
			local remaining = entity:flush(skip_fkeys)
			if remaining > 0 then
				next_flush[n] = entity
				total = total + remaining
			end
		end
	until not skip_fkeys and (total == 0 or total == prev)

	if total > 0 then
		error("Was not able to flush all records, likely caused by an uncaught circular dependency")
	end

	pending_changes = false
end

function em.flush()
	em.begin(true)
	local success, err = pcall(em.raw_flush)
	if success then
		em.commit(true)
	else
		em.rollback()
		error("Failed to flush:\n"..err)
	end
end

-- rollback changes
function em.rollback()
	confirm(em.db:exec("ROLLBACK TRANSACTION"), "Failed to rollback transaction")

	for hook in pairs(transaction.update) do
		hook(false)
	end

	transaction = nil
end

-- check if currently in a transaction
function em.transaction()
	return transaction ~= nil
end

function em.pending_changes()
	return pending_changes
end


-----------------------------------
-- Entity creation and functions --
-----------------------------------

local function new_row(entity, data, reread)
	-- current values
	local values = {}

	-- values per the current transaction
	local updated = {}

	-- whether we've been deleted or not
	local deleted = false

	-- whether our flush() has been committed or not
	local dirty = transaction and data.rowid == nil

	-- cache fields
	local fields = entity.fields

	-- for later
	local initial_lookup = {}

	-- row object
	local mt = {}
	local row = setmetatable({}, mt)

	for k,v in pairs(fields) do
		if v.required and data[k] == nil then
			error("Required field "..k.." is missing ("..entity.name..")")
		end

		local value, lookup = transform_value(v, data[k], entity, row)

		initial_lookup[k] = lookup

		if reread then
			updated[k] = value
		else
			values[k] = value
		end
	end

	if reread then
		updated.rowid = data.rowid
	else
		values.rowid = data.rowid
	end

	-- commit/rollback hook
	local function hook(is_commit)
		if is_commit then
			for k,v in pairs(updated) do
				values[k] = v
			end

			local rowid = values.rowid
			entity.rows[rowid] = row

			updated = {}
			reread = nil

			if not entity.dirty[row] then
				dirty = false
			end
		else
			updated = {}

			if reread then
				local data = reread()
				dirty = values.data == nil
				for k,v in pairs(data) do
					values[k] = v
				end
				reread = nil
			end

			if dirty then
				entity.dirty[row] = true
			end
		end
	end

	-- merge row changes resulting from a flush()
	local function merge(update)
		local target

		if transaction then
			target = updated
			transaction.update[hook] = true
		else
			target = values
		end

		for k,v in pairs(update) do
			target[k] = v
		end
	end

	-- get a row value
	local function value(self, key)
		local rv

		if deleted then
			error("This row has already been deleted")
		end

		rv = updated[key]
		if rv == nil then
			rv = values[key]
		end

		return rv
	end

	local function raw(self, key)
		local rv = value(self, key)

		if type(rv) == "table" then
			rv = rv:raw(rv.entity.key)
		elseif type(rv) == "function" then
			rv = rv(false)
		end

		return rv
	end

	-- get a row value and fetch it
	local function get(self, key)
		local field = fields[key]

		if field == nil then
			if key == "rowid" then
				return value(self, key)
			end

			return nil
		end

		if field.virtual then
			local get = get_vfkey(entity, field)

			if get == nil then
				error("Can't access "..entity.name.."."..field.name.." child table "..field.entity)
			end

			local pkey = raw(self, entity.key)

			return get(pkey)
		end

		local rv = value(self, key)

		if rv == nil then
			return nil
		end

		if field.class == "ENTITY" and type(rv) ~= "table" then
			local other = entities[field.entity]

			rv = other:get(rv)
		elseif type(rv) == "function" then
			rv = rv(true)
		end

		return rv
	end

	local function check_collision(key, lookup)
		if entity.caches[key][lookup] then
			return true
		end

		local has = entity.statements.has[key]()
		confirm(has:bind(1, lookup), "Failed to bind unique field")
		if execute(has, get_first) ~= 0 then
			return true
		end

		return false
	end

	-- update a row value
	local function set(self, key, value)
		if deleted then
			error("This row has already been deleted")
		elseif fields[key] == nil then
			error("Invalid field "..key.." on table "..entity.name)
		end

		local prev = values[key]
		if prev ~= value then
			local field = fields[key]

			local store, lookup = transform_value(field, value, entity, row)

			if lookup ~= nil and fields[key].unique then
				if check_collision(key, lookup) then
					error("Field "..entity.name.."."..key.." value "..value.." already exists on "..entity.name)
				end

				local _, prev_lookup = transform_value(field, prev, entity, row)

				entity.caches[key][prev] = nil
				entity.caches[key][lookup] = row
			end

			values[key] = store
			mark_dirty(entity, row)
		end
	end

	-- row members
	local members = {
		-- explicit get (fetches entities)
		get = get,
		-- explicit get (does not fetch entities)
		raw = raw,
		-- explicit set
		set = set,
		-- the row's entity
		entity = entity,
		-- whether the row's been deleted or not
		deleted = function(self)
			return deleted
		end,
		-- flush changes to the db
		flush = function(self, skip_fkeys)
			local skipped

			if entity.dirty[row] then
				local statement
				local code

				local rowid = value(self, "rowid")

				if deleted then
					statement = entity.statements.delete()
					confirm(statement:bind(1, rowid), "Failed to bind rowid")
					execute(statement)
					return true
				end

				if rowid == nil then
					statement = entity.statements.insert()
				else
					statement = entity.statements.update()

					confirm(statement:bind(#entity.field_names + 1, rowid), "Failed to bind rowid")
				end

				for i,name in ipairs(entity.field_names) do
					local field = fields[name]
					local value = updated[name]
					if value == nil then
						value = values[name]
					end
					local call = statement.bind

					if field.class == "BLOB" then
						call = statement.bind_blob
					elseif field.class == "ENTITY" and type(value) == "table" then
						if skip_fkeys and not field.required and value.rowid == nil then
							value = nil
							skipped = true
						else
							value = value:raw(value.entity.key)

							if value == nil then
								return false
							end
						end

						merge{[name]=value}
					elseif type(value) == "function" then
						value = value(false)
					end

					confirm(call(statement, i, value), "Failed to bind "..name.." to parameter #"..i)
				end

				repeat
					code = step(statement)

					if code ~= sqlite3.ROW and code ~= sqlite3.DONE then
						statement:reset()
						error("Failed to call statement: "..code)
					end
				until code == sqlite3.DONE

				if rowid == nil then
					local new_rowid = statement:last_insert_rowid()
					merge{rowid = new_rowid}

					local pkey = entity.key
					local field = entity.fields[pkey]
					if field ~= nil and field.class == "ID" then
						merge{[pkey] = new_rowid}
					end
				end

				statement:reset()

				entity.dirty[row] = false

				if not skipped then
					if transaction then
						dirty = true
					else
						dirty = false
					end
				end
			end

			return not skipped
		end,

		-- delete the row
		delete = function(self)
			if deleted then
				return
			end

			if updated.rowid == nil and values.rowid == nil then
				entity.dirty[row] = nil
			else
				mark_dirty(entity, row)
			end

			deleted = true
		end,

		-- pairs() on fields
		fields = function(self)
			local f, s, first = pairs(fields)

			local function next_field(s, prev)
				local name = f(s, prev)
				return name, get(self, name)
			end

			return next_field, s, first
		end,

		-- for debugging purposes
		__debug = function(self)
			return {
				values = values,
				updated = updated,
				deleted = deleted,
				dirty = dirty,
				members = members
			}
		end,
	}

	function mt:__index(key)
		local rv = members[key]

		if rv ~= nil or type(key) ~= "string" then
			return rv
		end

		local lower = key:lower()
		if fields[lower] or key == "rowid" then
			return get(self, lower)
		end

		local name = lower:match("^_(.+)$")
		if name then
			return raw(self, name)
		end

		return nil
	end

	function mt:__newindex(key, value)
		set(self, key, value)
	end

	if reread and transaction then
		transaction.update[hook] = true
	end

	-- cache the row
	for i,name in ipairs(entity.unique_fields) do
		if data[name] ~= nil then
			local lookup = initial_lookup[name]

			entity.caches[name][lookup] = row
		end
	end
	if data.rowid then
		entity.rows[data.rowid] = row
	end

	return row
end

---------------------
-- Table functions --
---------------------

-- create a new row
function entity:new(data, skip_check)
	data = data or {}

	if not skip_check then
		for k,v in pairs(data) do
			local field = self.fields[k]

			if not field then
				error("Invalid field: "..k)
			end
		end
	end

	local is_unique = self.statements and self.statements.is_unique and self.statements.is_unique()
	if is_unique then
		for i,name in ipairs(self.unique_fields) do
			local _, lookup = transform_value(self.fields[name], data[name])

			if self.caches[name][lookup] then
				error("UNIQUE constraint broken")
			end

			confirm(is_unique:bind(i, value), "Failed to bind unique field")
		end

		local values = execute(is_unique, is_unique.get_values)

		if values and values[1] ~= 0 then
			error("UNIQUE constraint broken")
		end
	end

	local row = new_row(self, data) 

	mark_dirty(self, row)
	
	return row
end

-- check if a row exists
function entity:has(key)
	if self.cache[key] ~= nil then
		return true
	end

	local statement = self.statements.exists()

	confirm(statement:bind(1, key), "Failed to bind primary key")

	local values = execute(statement, statement.get_values)

	return values and values[1] ~= 0
end

local function entity_reader(self, statement, key)
	return function()
		local field_names = self.field_names

		confirm(statement:bind(1, key), "Failed to bind primary key")

		local values = {}

		local data = execute(statement, statement.get_values)

		if data == nil then
			return nil
		end

		for i,name in ipairs(field_names) do
			values[name] = data[i]
		end

		values.rowid = data[#field_names + 1]

		return values
	end
end

-- common method for running a statement to fetch a row from the db
local function get_entity(self, statement, value)
	-- make this a sub-function so we can re-use it if needed (transaction rollback scenario)
	local read = entity_reader(self, statement, value)

	local values = read()

	if values == nil then
		return nil
	end

	if not transaction then
		read = nil
	end

	local row = new_row(self, values, read)

	return row
end

-- get a row by pkey
function entity:get(key)
	local rv = self.cache[key]
	if rv ~= nil then
		return rv
	end

	return get_entity(self, self.statements.get(), key)
end

-- flush all the dirty entities on a table
function entity:flush(skip_fkeys)
	local dirty = self.dirty
	local remaining = 0

	for row in pairs(dirty) do
		if row:flush(skip_fkeys) then
			dirty[row] = nil
		else
			remaining = remaining + 1
		end
	end

	return remaining
end

-- generate CREATE TABLE sql dynamically
function entity:create_sql()
	local header = "CREATE TABLE IF NOT EXISTS \""..self.name.."\" (\n\t"
	local footer = "\n)"

	local lines = {}
	local fkeys = {}
	for _, name in ipairs(self.field_names) do
		local field = self.fields[name]
		local class = field.class
		local sqltype = field.sqltype or class

		name = quote(name)

		if class == "ENTITY" then
			local entity = entities[field.entity]
			if entity == nil then
				error("Table "..self.name.."."..name.." references non-existent table "..field.entity)
			end
			if entity.key == "rowid" then
				error("Table "..self.name.."."..name.." references keyless table "..field.entity)
			end
			local pkey = entity.fields[entity.key]
			sqltype = pkey.class
			table.insert(fkeys, {name, quote(field.entity), quote(entity.key)})
		end

		local parts = { name, sqltype }

		if field.required then
			table.insert(parts, "NOT NULL")
		end

		if field.unique then
			table.insert(parts, "UNIQUE")
		end

		table.insert(lines, table.concat(parts, " "))
	end

	for _,data in ipairs(fkeys) do
		local field, entity, fkey = unpack(data)
		table.insert(lines, "FOREIGN KEY("..field..") REFERENCES "..entity.."("..fkey..")"..
			" ON UPDATE CASCADE ON DELETE CASCADE")
	end

	if self.key ~= "rowid" then
		table.insert(lines, "PRIMARY KEY("..quote(self.key)..")")
	end

	return header..table.concat(lines, ",\n\t")..footer
end

-- create the table on the DB
function entity:create()
	confirm(em.db:exec(self:create_sql()), "Failed to create table "..self.name)
	prepare_statements(self)

	return true
end

----------------------
-- Query statements --
----------------------

local query = {}

local query_functions = {
	lua = {
		param = function(context, a)
			return function(row, params)
				return params[a]
			end
		end,
		field = function(context, a)
			return function(row, params)
				return row:raw(a)
			end
		end,
		const = function(context, a)
			return function(row, params)
				return a
			end
		end,
		uni = {
			is_null = function(context, a)
				return function(row, params)
					return a(row, params) == nil
				end
			end,
			is_not_null = function(context, a)
				return function(row, params)
					return a(row, params) ~= nil
				end
			end,
		},
		bi = {
			[">"] = function(context, a, b)
				return function(row, params)
					return a(row, params) > b(row, params)
				end
			end,
			[">="] = function(context, a, b)
				return function(row, params)
					return a(row, params) >= b(row, params)
				end
			end,
			["<"] = function(context, a, b)
				return function(row, params)
					return a(row, params) < b(row, params)
				end
			end,
			["<="] = function(context, a, b)
				return function(row, params)
					return a(row, params) <= b(row, params)
				end
			end,
			["="] = function(context, a, b)
				return function(row, params)
					local a = a(row, params)
					local b = b(row, params)
					return a ~= nil and b ~= nil and a == b
				end
			end,
			["~="] = function(context, a, b)
				return function(row, params)
					local a = a(row, params)
					local b = b(row, params)
					return a == nil or b == nil or a ~= b
				end
			end,
		},
		aggregates = {
			any = function(context, query)
				if #query == 1 then
					return query[1]
				end

				return function(row, params)
					for _,f in ipairs(query) do
						if f(row, params) then
							return true
						end
					end
					return false
				end
			end,
			all = function(context, query)
				if #query == 1 then
					return query[1]
				end

				return function(row, params)
					for _,f in ipairs(query) do
						if not f(row, params) then
							return false
						end
					end
					return true
				end
			end,
		}
	},
	sql = {
		param = function(context, a)
			return ":"..a
		end,
		field = function(context, a)
			return quote(a)
		end,
		const = function(context, a)
			local n = context.const_count + 1
			context.constants["_"..n] = a
			context.const_count = n
			return ":_"..n
		end,
		uni = {
			is_null = function(context, a)
				return a.." IS NULL"
			end,
			is_not_null = function(context, a)
				return a.." IS NOT NULL"
			end,
		},
		bi = {
			[">"] = function(context, a, b)
				return a.." > "..b
			end,
			[">="] = function(context, a, b)
				return a.." >= "..b
			end,
			["<"] = function(context, a, b)
				return a.." < "..b
			end,
			["<="] = function(context, a, b)
				return a.." <= "..b
			end,
			["="] = function(context, a, b)
				return a.." = "..b
			end,
			["~="] = function(context, a, b)
				return a.." <> "..b
			end,
		},
		aggregates = {
			any = function(context, query)
				if #query == 1 then
					return query[1]
				end

				return "("..table.concat(query, " OR ")..")"
			end,
			all = function(context, query)
				if #query == 1 then
					return query[1]
				end

				return "("..table.concat(query, " AND ")..")"
			end,
		}
	}
}

if jsonlib then
	query_functions.lua.json = function(context, a, b)
		local path = {}
		for str in b:gmatch("[^.]+") do
			table.insert(path, str)
		end
		return function(row, params)
			local obj = row:get(a)
			for _,str in ipairs(path) do
				if type(obj) ~= "table" then
					return nil
				end

				obj = obj[str]
			end

			return obj
		end
	end
	query_functions.sql.json = function(context, a, b)
		return "json_extract("..a..", '$."..b.."')"
	end
end

local function parse_query_value(class, context, value)
	if type(value) == "table" then
		return class.const(context, value[1])
	elseif type(value) == "string" then
		value = value:lower()

		local param = value:match("^:(.+)$")
		if param and not param:match("^_") then
			return class.param(context, param)
		end

		if context.entity.fields[value] then
			return class.field(context, value)
		end

		if jsonlib then
			local field, path = value:match("^([^.]+)[.](.+)$")
			if field and context.entity.fields[field] and context.entity.fields[field].class == "JSON" then
				return class.json(context, field, path)
			end
		end

		local text = value:match("^'(.+)'$")
		if text then
			return class.const(context, text)
		end
	end

	return class.const(context, value)
end

local function parse_query(class, context, query)
	if type(query) == "string" then
		local array ={}
		for word in query:gmatch("%S+") do
			table.insert(array, word)
		end
		query = array
	end

	local len = #query
	
	if len == 2 then
		local a, b = unpack(query)
		local f = class.uni[a]
		if f then
			b = parse_query_value(class, context, b)
			return f(context, b)
		end
	elseif len == 3 then
		local a, b, c = unpack(query)
		local f = class.bi[b]
		if f then
			a = parse_query_value(class, context, a)
			c = parse_query_value(class, context, c)
			return f(context, a, c)
		end
	end

	local f = class.aggregates[query[1]]
	if f then
		local queries = {}
		for i=2,len do
			table.insert(queries, parse_query(class, context, query[i]))
		end

		return f(context, queries)
	end

	error("Invalid query.")
end

local function query_call(self, values)
	if transaction then
		error("Running query() clauses in transactions currently isn't supported")
	end

	local statement = self.statement()
	local entity = self.entity
	local field_names = entity.field_names
	local key = entity.key
	local cached_rows = entity.rows

	local parameters = {}
	if values then
		for k,v in pairs(values) do
			parameters[k] = v
		end
	end
	for k,v in pairs(self.constants) do
		parameters[k] = v
	end

	confirm(statement:bind_names(parameters), "Failed to bind parameters")

	local rows = execute_multi(statement)
	local set = {}

	for i,row in ipairs(rows) do
		local data = {}

		for j,name in ipairs(field_names) do
			data[name] = row[j]
		end

		data.rowid = row[#field_names + 1]

		local result = cached_rows[data.rowid]
		if result == nil then
			result = new_row(entity, data)
		end

		set[result] = true
	end

	local test = self.test
	for row in pairs(entity.dirty) do
		if not row:deleted() and test(row, values) then
			set[row] = true
		else
			set[row] = nil
		end
	end

	local results = {}
	for row in pairs(set) do
		table.insert(results, row)
	end

	return results
end

local query_mt = {__index = query, __call = query_call}

function entity:query(...)
	local query = setmetatable({}, query_mt)

	local field_names = self.field_names

	local quoted_field_names = {}
	for i,v in ipairs(field_names) do
		quoted_field_names[i] = quote(v)
	end

	local first = ...

	local params
	if type(first) == "string" and query_functions.lua.aggregates[first] ~= nil then
		params = {...}
	else
		params = {"all", ...}
	end

	query.entity = self
	query.constants = {}
	query.const_count = 0

	query.sql = "SELECT "..table.concat(quoted_field_names, ",")..",\"rowid\" FROM "..quote(self.name).." WHERE "..parse_query(query_functions.sql, query, params)

	query.statement = prepare{query.sql}

	query.test = parse_query(query_functions.lua, query, params)

	return query
end


return em
