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

local sqlite3 = require("lsqlite3")

-- module
local em = {}

-- module variables
local entities = {}
local transaction = nil
local check_statement = nil

-- entity metatable
local entity = {}
local entity_mt = { __index = entity }

-----------------------
-- Utility functions --
-----------------------

-- Enquote something for safer SQL
local function quote(name)
	return "\""..name.."\""
end

-- Execute a statement that only has zero or one result
local function execute(statement, f)
	local result, rv

	if f then
		result = statement:step()
		if result == sqlite3.DONE then
			return nil
		elseif result ~= sqlite3.ROW then
			statement:reset()
			error("Unexpected step result "..result)
		end

		rv = f(statement)
	end

	result = statement:step()
	if result ~= sqlite3.DONE then
		statement:reset()
		error("Unexpected step result "..result)
	end

	statement:reset()

	return rv
end

local function get_first(statement)
	local values = statement:get_values()

	return values and values[1]
end

-- confirm that a sqlite3 call worked
local function confirm(result, error_msg, ...)
	local acceptable = table.pack(...)
	local n = acceptable.n
	if n == 0 then
		acceptable = { sqlite3.OK }
		n = 1
	end

	for i=1,n do
		if result == acceptable[i] then
			return
		end
	end

	if error_msg == nil then
		error_msg = "Sqlite error"
	end

	error(error_msg.." (#"..result..")")
end

local cache_mt = { __mode = "v" }
local function cache()
	return setmetatable({}, cache_mt)
end

local weakmap_mt = { __mode = "k" }
local function weakmap()
	return setmetatable({}, weakmap_mt)
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

		result.name = name

		if options then
			if type(options) == "string" then
				if result.class == "ID" then
					options = {
						required = options:match("!"),
					}
				else
					options = {
						required = not options:match("?"),
						unique = options:match("!"),
					}
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
	id      = class{class="ID", unique=true},
}

-- verbose
em.class = classes

-- shortcut
em.c = classes

-- Entity classes (foreign keys), can accept name or table object
local function entity_class(entity, ...)
	if type(entity) == "table" then
		entity = entity.name
	end

	return class{class="ENTITY", entity=entity }(...)
end
em.entity = entity_class


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
				error("Database is closed.")
			end

			statement = em.db:prepare(sql)

			if statement == nil then
				error("Failed to prepare statement: "..sql)
			end
		end

		return statement
	end
end

-- global prepared statements
check_statement = prepare{"SELECT count(1) FROM \"sqlite_master\" WHERE type='table' AND name=?"}

local function prepare_statements(entity)
	local name = entity.name

	local check = check_statement()

	confirm(check:bind(1, name), "Failed to bind table name")
	if execute(check, get_first) == 0 then
		return
	end

	name = quote(name)

	local field_names = {}
	for i,v in ipairs(entity.field_names) do
		field_names[i] = quote(v)
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
			"VALUES ("..string.rep("?", #field_names, ", ")..")",
		},
		update = prepare{
			"UPDATE "..name,
			"SET ("..table.concat(field_names, ",")..")",
			"= ("..string.rep("?", #field_names, ", ")..")",
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
	em.db = sqlite3.open(filename)
end

-- create a new table
function em.new(entity_name, key, fields, options)
	if entities[entity_name] then
		error("Table "..entity_name.." already exists.")
	end

	local self = setmetatable({}, entity_mt)

	-- initial parsing
	local parsed = {}
	local field_names = {}

	if fields[1] then
		-- ordered fields
		for i,field in ipairs(fields) do
			local name = string.lower(field.name)
			if name == nil then
				error("Table "..name.." field #"..i.." is missing a name.")
			end

			parsed[name] = field
			field_names[i] = name
		end
	else
		-- unordered fields - pkey goes first
		field_names[1] = key

		for name,field in pairs(fields) do
			name = string.lower(name)

			parsed[name] = field

			if name ~= key then
				table.insert(field_names, name)
			end
		end
	end

	fields = parsed

	-- broad verifications
	if fields.rowid then
		error("Cannot overwrite rowid field.")
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
			error("Fields must be strings.")
		end

		-- string fields for convenience
		if type(field) == "string" then
			local tag, flags = field:match("^(.-)([?!]*)$")

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
				error("ID can only be used for primary keys.")
			end
		end
	end

	while #queue > 0 do
		local name = table.remove(queue)
		if name == entity_name then
			error("Circular dependency between "..entity_name.." and itself.")
		end
		local entity = entities[name]
		if entity ~= nil and not dependencies[entity] then
			dependencies[entity] = true

			if entity.fields == nil then
				error("Bad table "..name.." from "..tale_name)
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
			error("Already began a transaction.")
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
		error("No transaction.")
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
	confirm(em.db:exec("ROLLBACK TRANSACTION"), "Failed to rollback transaction.")

	for hook in pairs(transaction.update) do
		hook(false)
	end

	transaction = nil
end

-- check if currently in a transaction
function em.transaction()
	return transaction ~= nil
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

	for k,v in pairs(fields) do
		if v.required and data[k] == nil then
			error("Required field "..k.." is missing ("..entity.name..")")
		end

		if reread then
			updated[k] = data[k]
		else
			values[k] = data[k]
		end
	end

	local mt = {}

	local row = setmetatable({}, mt)

	-- commit/rollback hook
	local function hook(is_commit)
		if is_commit then
			for k,v in pairs(updated) do
				values[k] = v
			end

			local rowid = updated.rowid
			if rowid ~= nil and entity.key == "rowid" then
				entity.cache[rowid] = row
			end

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
	local function raw(self, key)
		local rv

		if deleted then
			error("This row has already been deleted.")
		end

		rv = updated[key]
		if rv == nil then
			rv = values[key]
		end

		return rv
	end

	-- get a row value and fetch it
	local function get(self, key)
		local rv = raw(self, key)

		if rv == nil then
			return nil
		end

		local field = fields[key]
		if field and field.class == "ENTITY" and type(rv) ~= "table" then
			local other = entities[field.entity]

			rv = other:get(rv)
		end

		return rv
	end

	local function check_collision(key, value)
		if entity.caches[key][value] then
			return true
		end

		local has = entity.statements.has[key]()
		confirm(has:bind(1, value), "Failed to bind unique field")
		if execute(has, get_first) ~= 0 then
			return true
		end

		return false
	end

	-- update a row value
	local function set(self, key, value)
		if deleted then
			error("This row has already been deleted.")
		elseif fields[key] == nil then
			error("Invalid field "..key.." on table "..entity.name)
		end

		local prev = values[key]
		if prev ~= value then

			if fields[key].unique then
				if check_collision(key, value) then
					error("Field "..key.." value "..value.." already exists on "..entity.name)
				end

				entity.caches[key][prev] = nil
				entity.caches[key][value] = row
			end

			values[key] = value
			entity.dirty[row] = true
		end
	end

	-- row functions
	local functions = {
		-- explicit get (fetches entities)
		get = get,
		-- explicit get (does not fetch entities)
		raw = raw,
		-- explicit set
		set = set,
		-- flush changes to the db
		flush = function(self, skip_fkeys)
			local skipped

			if entity.dirty[row] then
				local statement
				local result

				local rowid = raw(self, "rowid")

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
							local key = entities[field.entity].key
							value = value:get(key)

							if value == nil then
								return false
							end
						end

						merge{[name]=value}
					end

					confirm(call(statement, i, value), "Failed to bind "..name.." to parameter #"..i)
				end

				repeat
					result = statement:step()

					if result ~= sqlite3.ROW and result ~= sqlite3.DONE then
						statement:reset()
						error("Failed to call statement: "..result)
					end
				until result == sqlite3.DONE

				if rowid == nil then
					merge{rowid = statement:last_insert_rowid()}
				end

				statement:reset()

				entity.dirty[row] = false

				if not skip_fkeys then
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

			-- uninserted
			if updated.rowid == nil and values.rowid == nil then
				entity.dirty[row] = nil
			else
				entity.dirty[row] = true
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
				functions = functions
			}
		end,
	}

	function mt:__index(key)
		local rv = functions[key]

		if rv ~= nil then
			return rv
		end

		if fields[key] then
			return get(self, key)
		end

		local lower = key:lower()
		if fields[lower] then
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

	return row
end

---------------------
-- Table functions --
---------------------

-- create a new row
function entity:new(data, skip_check)
	if not skip_check then
		for k,v in pairs(data) do
			if not self.fields[k] then
				error("Invalid field: "..k)
			end
		end
	end

	local is_unique = self.statements and self.statements.is_unique and self.statements.is_unique()
	if is_unique then
		for i,name in ipairs(self.unique_fields) do
			local value = data[name]

			if self.caches[name][value] then
				error("UNIQUE constraint broken.")
			end

			confirm(is_unique:bind(i, value), "Failed to bind unique field")
		end

		local values = execute(is_unique, is_unique.get_values)

		if values and values[1] ~= 0 then
			error("UNIQUE constraint broken.")
		end
	end

	local row = new_row(self, data) 

	for i,name in ipairs(self.unique_fields) do
		local value = data[name]
		self.caches[name][value] = row
	end

	self.dirty[row] = true
	
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
			local name = field_names[i] or "rowid"
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

	for i,name in ipairs(self.unique_fields) do
		self.caches[name][values[name]] = row
	end

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
			class = pkey.class
			table.insert(fkeys, {name, quote(field.entity), quote(entity.key)})
		elseif class == "ID" then
			class = "INTEGER"
		end

		local parts = { name, class }

		if field.required then
			table.insert(parts, "NOT NULL")
		end

		if field.unique then
			table.insert(parts, "UNIQUE")
		end

		table.insert(lines, table.concat(parts, " "))
	end

	for _,data in ipairs(fkeys) do
		local field, entity, fkey = table.unpack(data)
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

function entity:where(clause)
	local field_names = self.field_names

	local quoted_field_names = {}
	for i,v in ipairs(field_names) do
		quoted_field_names[i] = quote(v)
	end
	
	local sql = "SELECT "..table.concat(quoted_field_names)..",\"rowid\" FROM "..quote(self.name).." WHERE "..clause

	local factory = prepare{sql}

	return function(...)
		if transaction then
			error("Running where() clauses in transactions currently isn't supported.")
		end

		local statement = factory()

		confirm(statement:bind_values(...), "Failed to bind values")

		local results = {}

		-- just store the data until we can reset()
		for row in statement:rows() do
			table.insert(results, row)
		end

		statement:reset()

		for i,row in ipairs(results) do
			local data = {}

			for j,value in ipairs(row) do
				local field = field_names[j] or "rowid"

				data[field] = value
			end

			results[i] = new_row(self, data)
		end

		return results
	end
end


return em
