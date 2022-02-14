em = require "em"

em.open("a.db")

a = em.new("a", "ak", {
	ak = em.c.text,
	desc = em.c.text,
})

b = em.new("b", "bk", {
	bk = em.c.int,
	parent = "a",
})

c = em.new("c", "name", {
	parent = a,
	name = em.c.text,
	desc = em.c.text("?"),
})

d = em.new("d", "name", {
	name = em.c.text("!"),
	mail = em.c.text("!?"),
})

b_for = b:where("parent = ?")
