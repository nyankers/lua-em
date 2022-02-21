EXPORT_ASSERT_TO_GLOBALS = true

local lu = require("luaunit")
local db = nil
local weak = setmetatable({}, { __mode = "v" })

----------------------------
-- Test Table Definitions --
----------------------------

--local e = {
--	children = em.new("children", "id", { id = em.c.id, parent = "parent", value = em.c.text }),
--	optional = em.new("optional", "id", { id = em.c.id, req = em.c.text, opt = em.c.text("?") }),
--}

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

	parent = em.new("parent", "key", { key = em.c.text, name = em.c.text, child = "child*", children = "children*" })
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

function test_children()
end

-- last line
os.exit(lu.LuaUnit.run())
