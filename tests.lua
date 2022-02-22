EXPORT_ASSERT_TO_GLOBALS = true

local lu = require("luaunit")
local weak = setmetatable({}, { __mode = "v" })

----------------
-- Unit Tests --
----------------

function test_insert()
	local em = dofile("em.lua")
	em.open()
	
	local map = em.new("map", "key", { key = em.c.text, value = em.c.text })
	map:create()

	local tmp = map:new{key="a", value="b"}
	weak.tmp = tmp
	tmp = nil

	em.flush()
	collectgarbage()

	-- should be cleared from memory now
	assertIsNil(weak.tmp)

	tmp = map:get("a")
	assertNotNil(tmp)

	assertEquals(tmp.key, "a")
	assertEquals(tmp.value, "b")
end

function test_child()
	local em = dofile("em.lua")
	em.open()

	parent = em.new("parent", "key", { key = em.c.text, name = em.c.text, child = "child*" })
	parent:create()

	child = em.new("child", "parent", { parent = "parent!", data = em.c.text })
	child:create()

	weak.a = parent:new{key="a", name="foo"}
	weak.b = parent:new{key="b", name="bar"}
	weak.kid = child:new{parent="a", data="blah"}

	em.flush()
	collectgarbage()

	-- should be cleared from memory now
	assertIsNil(weak.a)
	assertIsNil(weak.b)
	assertIsNil(weak.kid)

	local a = parent:get("a")
	assertNotNil(a)

	local b = parent:get("b")
	assertNotNil(b)

	local kid = a.child
	assertNotNil(kid)
	assertIsNil(b.child)

	assertEquals(kid.data, "blah")
	assertEquals(kid.parent, a)
	assertEquals(kid._parent, "a")
	assertEquals(kid.parent.name, "foo")

	-- set via table
	kid.parent = b
	assertIsNil(a.child)
	assertEquals(b.child, kid)
	assertEquals(kid.parent, b)
	assertEquals(kid._parent, "b")
	assertEquals(kid.parent.name, "bar")

	weak.kid = kid
	kid = nil

	em.flush()
	collectgarbage()
	assertIsNil(weak.kid)

	local kid = child:get("b")
	
	assertNotNil(kid)
	assertEquals(kid, b.child)

	-- set via key
	kid.parent = "a"
	assertIsNil(b.child)
	assertEquals(a.child, kid)
	assertEquals(kid.parent, a)
	assertEquals(kid._parent, "a")
	assertEquals(kid.parent.name, "foo")

	weak.kid = kid
	kid = nil

	em.flush()
	collectgarbage()
	assertIsNil(weak.kid)

	assertIsNil(b.child)
	assertNotNil(a.child)
	assertEquals(a.child.data, "blah")
end

function test_data_types()
	local em = dofile("em.lua")
	em.open()

	local entity = em.new("entity", "id", {
		id = em.c.id,
		text = em.c.text,
		numeric = em.c.numeric,
		int = em.c.int,
		real = em.c.real,
		blob = em.c.blob,
	})
	entity:create()

	local row = entity:new{
		id = 1,
		text = "ok",
		numeric = 1.2,
		int = 1,
		real = 1.4,
		blob = "blah blah"
	}
	em.flush()

	assertEquals(entity:get(1), row)
	assertIsNil(entity:get(2))

	assertEquals(row.id, 1)
	assertEquals(row.text, "ok")
	assertEquals(row.numeric, 1.2)
	assertEquals(row.int, 1)
	assertEquals(row.real, 1.4)
	assertEquals(row.blob, "blah blah")

	row.id = "2"
	row.text = 5
	row.numeric = "7.1"
	row.int = "5.2"
	row.real = "9.7"
	row.blob = 42
	em.flush()

	assertEquals(entity:get(2), row)
	assertIsNil(entity:get(1))

	assertEquals(row.id, 2)
	assertEquals(row.text, "5")
	assertEquals(row.numeric, 7.1)
	assertEquals(row.int, 5)
	assertEquals(row.real, 9.7)
	assertEquals(row.blob, "42")

	assertError(function() row.id = "blah" end)
	assertError(function() row.numeric = "blah" end)
	assertError(function() row.int = "blah" end)
	assertError(function() row.real = "blah" end)

	for name in row:fields() do
		assertError(function() row[name] = {} end)
		assertError(function() row[name] = function() end end)
		assertError(function() row[name] = em.db end)
		assertError(function() row[name] = coroutine.create(function() end) end)
	end
end

function test_on_change()
	local em = dofile("em.lua")
	em.open()

	local count = 0
	em.on_change = function()
		count = count + 1
	end

	local entity = em.new("test", "id", { id = em.c.id, value = em.c.text })
	entity:create()

	assertEquals(count, 0)

	entity:new{id=1, value="foo"}
	assertEquals(count, 1)

	entity:new{id=2, value="bar"}
	assertEquals(count, 1)

	em.flush()

	entity:new{id=3, value="bar"}
	assertEquals(count, 2)
end

function test_children()
	local em = dofile("em.lua")
	em.open()

	parent = em.new("parent", "key", { key = em.c.text, name = em.c.text, children = "child*" })
	parent:create()

	child = em.new("child", "id", { id = em.c.id, parent = "parent", data = em.c.text })
	child:create()

	weak.a = parent:new{key="a", name="foo"}
	weak.b = parent:new{key="b", name="bar"}
	weak.kid1 = child:new{id=1, parent="a", data="abc"}
	weak.kid2 = child:new{id=2, parent="a", data="def"}
	weak.kid3 = child:new{id=3, parent="a", data="ghi"}
	weak.kid4 = child:new{id=4, parent="a", data="jkl"}
	weak.kid5 = child:new{id=5, parent="a", data="mno"}

	em.flush()
	collectgarbage()

	-- should be cleared from memory now
	assertIsNil(weak.a)
	assertIsNil(weak.b)
	assertIsNil(weak.kid1)
	assertIsNil(weak.kid2)
	assertIsNil(weak.kid3)
	assertIsNil(weak.kid4)
	assertIsNil(weak.kid5)

	local a = parent:get("a")
	assertNotNil(a)

	local b = parent:get("b")
	assertNotNil(b)

	local kids = a.children
	assertIsTable(kids)
	assertEquals(#kids, 5)

	weak.kid = kids[1]
	kids = nil
	collectgarbage()
	assertIsNil(weak.kid)

	local kid1 = child:get(1)
	local kid2 = child:get(2)
	local kid3 = child:get(3)
	local kid4 = child:get(4)
	local kid5 = child:get(5)
	assertNotNil(kid1)
	assertNotNil(kid2)
	assertNotNil(kid3)
	assertNotNil(kid4)
	assertNotNil(kid5)

	assertItemsEquals(a.children, {kid1, kid2, kid3, kid4, kid5})

	local kid6 = child:new{id=6, parent="a", data="pqr"}
	
	assertItemsEquals(a.children, {kid1, kid2, kid3, kid4, kid5, kid6})
	assertEquals(b.children, {})

	kid1.parent = "b"

	assertItemsEquals(a.children, {kid2, kid3, kid4, kid5, kid6})
	assertItemsEquals(b.children, {kid1})

	kid6.parent = "b"

	assertItemsEquals(a.children, {kid2, kid3, kid4, kid5})
	assertItemsEquals(b.children, {kid1, kid6})
end

-- last line
os.exit(lu.LuaUnit.run())
