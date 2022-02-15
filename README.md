# Sqlite Entity Manager

**Hierarchy**: `em` -> `entity` -> `row`

(Technically this is incorrect nomenclature, as entities refer to rows rather
than tables, but it's used here to avoid name collision with lua tables.)

## Features
* Changes are queued until a later flush() statement
* Foreign key fields automagically return the related row object

## Caveats
* Changing a UNIQUE field is sticky business. You may want to flush() the
  field directly afterward to prevent from reserving both the old field (on
  the DB) and new field (in memory). Ideally, avoid too many UNIQUES (beyond
  the primary key) where possible, but especially ones that change often.
* Circular required foreign keys aren't allowed (e.g. table A points to
  table B which points back to table A). Non-required foreign keys do not
  count toward this restriction.
* Field names are always lowercase, and "rowid" may not be used as a field
  name.

## Undertested:
* Transactions (e.g. reading fields during transactions, etc)
* `id` pkeys
* UNIQUE constraints (multiple per entity)

## Module

### Module Members

#### em.begin(strict)
Begins a transaction. If strict is true, it fails if one's already begun.

#### em.c
A table of basic field types, described in more detail below.

#### em.class
A more verbose alias for `m.c` above.

#### em.close()
Close the database if it's been opened. Warning: This does not flush changes
first. You almost certainly want to run em.flush() before running em.close().

#### em.commit(force)
Commits a transaction. Unless force it true, this merely unwinds a single
em.begin() statement, and the transaction is only committed once they're all
unwound.

#### em.db
The underlying sqlite db object if it's been opened; otherwise, nil.

#### em.entities() -> iterator
A pairs() style function for all the registered entities.

#### em.fkey(...)
A function for creating foreign key fields, described in more detail below.

#### em.flush()
Flushes all changes to the database in a strict transaction.

#### em.get(name) -> entity
Returns an existing entity by name.

#### em.new(name, key, fields, options) -> entity
Defines a new entity description, as described below. (Note: this does not
create it on the database, it only tells the entity manager that this entity
exists.)

#### em.open(filename)
Opens the database.

#### em.raw\_flush()
Flushes all changes to the database, but does not begin/commit/rollback
transactions.

#### em.rollback()
Rollbacks a transaction, no matter the depth.

#### em.transaction()
Returns true if there's an active transaction; otherwise, returns false.

#### em.version -> {major, minor, release}
A version array in the form of `{major, minor, release}`, e.g. version 0.1.0 would be `{0, 1, 0}`.

#### em.version\_string -> string
The version as a string, e.g. version 0.1.0 would be `"0.1.0"`.

### Module Registers

These are values that a user may set to control how the module behaves.

#### em.default\_key (string or nil)

Default: `nil`

When calling `em.new()` with a field `key`, this name is used when none is
given. If it's `nil` (or any other non-string value), then such attempts cause
an error to happen instead.

#### em.retry (integer, boolean, or function)

Default: `false`

This register tells the entity manager how to react when it receives a `BUSY`
code.

If a number is given, it will try that many times (so `em.retry = 1` will only
try once total).

If a function is given, it'll call the function, passing along the current
number of attempts. If the function returns true, then it will retry;
otherwise, it will not.

Otherwise, a truthy value will make it retry indefinitely, whereas a falsy
value will only try once.

Note that this register does not imply any waiting period. That can be done by
configuring the database directly if needed, by accessing `em.db`.


## Entities

An entity may be declared using `em.new(name, key, fields, options)` where:

| Argument  | Description                                       |
| --------- | ------------------------------------------------- |
| `name`    | The name of the table                             |
| `key`     | The primary key of the table, or nil to use rowid |
| `fields`  | An array or map of fields (optional)              |
| `options` | Reserved (optional)                               |

The table's `key` may be given either as a string or as a field per below. If a
string is used, then `fields` must contain a field with a matching name. If a
field is used, then either a name must be given (similar to passing fields in
as an array), or `em.default_key` must be set.

If `fields[1]` exists, then fields is used solely as an array, and each field
will need a name parameter.

Fields have type information and may even point to a different table:

| Lua constant   | SQLite equivalent |
| -------------- | ----------------- | 
| `em.c.text`    | TEXT              |
| `em.c.numeric` | NUMERIC           |
| `em.c.int`     | INT (not INTEGER) |
| `em.c.real`    | REAL              |
| `em.c.blob`    | BLOB              |
| `em.c.id`      | INTEGER (internally marked as ID) |

The above types are functions which may accept up to two parameters, a name and
an option table/string. If a single string parameter is given, it's assumed to
be a name if it has a single letter, and otherwise is treated as an option
string.

| Field option | Description                           |
| `required`   | Defaults to true, disabled with `"?"` |
| `unique`     | Defaults to false, enabled with `"!"` |
| `name`       | May be given in the options table     |

The `em.c.id` field behaves differently in two ways:

1. It must always be the primary key of the table.
2. required defaults to false (enabled with `"!"`), unique is always true
   (cannot be disabled).


Foreign key fields can be denoted in three ways:

1. `em.fkey(entity, ...)`, with parameters after `entity` working as the above
   field type functions.
2. A string with the entity's name, followed by any string options.
3. The entity object itself, though no options may be provided with this
   approach.

Foreign keys additionally have the following field options:

| Field option | Description                                                            |
| ------------ | ---------------------------------------------------------------------- |
| `entity`     | The name of the related entity, set automatically by the above methods |
| `virtual`    | Defines a virtual foreign key field if `true`, enabled with `"*"`      |
| `key`        | The key on the child table, only used by virtual foreign keys          |
| `multi`      | Whether a virtual foreign key will return multiple elements or not     |

In the case of `em.fkey()`, the entity parameter may be either an entity
object or a string. Foreign key fields may referenced by name before they're
introduced via em.new().

Foreign key fields automatically use the primary key of the related field.
They may be set to a row object of the related object directly, or to a
primitive value that represents one (e.g. `data.owner = "username"`)

Virtual foreign keys, denoted by the `virtual` field option, represent access
to child tables. The `key` option can be used when the child table has multiple
foreign keys to the parent table, but is otherwise unnecessary. A single
element or `nil` will be returned if the child's foreign key is unique;
otherwise, an array of elements will be returned. If the `multi` field option
is set to `true` or `false`, a runtime error will be given if this expectation
is not met.


### Entity methods

#### entity:create()
Runs `entity:create_sql()` on the database.

#### entity:create\_sql() -> sql
Returns SQL that can be used to create this table.

#### entity:flush(skip\_fkeys) -> remaining
Flushes all changed rows to the database. If `skip_fkeys` is true, then fields
that point to entities which aren't on the database yet will be nulled where
possible, but any such rows will still be marked dirty. Returns the number of
rows which still need to be saved.

#### entity:get(key) -> row
Returns a row with the given key, or nil if there isn't any.

#### entity:has(key) -> bool
Returns true if the entity has a row with the given key, otherwise false.

#### entity:new(data, skip) -> row
Creates a new row with a given data table, where keys must represent fields
on the table, and values should be appropriate to those fields. If skip is
true, then various safety checks will be skipped (useful if you know the
data is correct, for some reason).

#### entity:where(query) -> function
Creates a function using the given query to fetch all matching rows from the
database. The query should be assumed to follow SQL along the lines of
`SELECT * FROM entity WHERE `. Parameters may be given using sqlite's usual
numeric parameter approach, e.g. `entity:where("owner = ?")`.

The resulting function has a signature like `where(...)` accepts these
parameters and binds them in order they're given (using lsqlite3's
`stmt:bind_values(...)`).

**Note:** This function currently isn't supported in transactions. Furthermore,
the results will honor the database's current state, *not* any pending changes.
Thus, if this distinction would cause issues, run `em.flush()` before using
these functions.


## Rows

Rows may directly retrieve and set fields by name, in which case capitalization
does not matter. For example, if a row has a field called "name", then
`row.NAME`, `row.Name`, and `row.name` are logically equivalent. If a field
name clashes with a row function, the function is preferred, but functions are
case sensitive, so besides being able to directly call `row:get()` and
`row:set()`, you can also capitalize the field name to avoid this (e.g. if you
have a field called "get", you could access it using `row.GET`).

You can also prefix a field with an underscore (`_`) to access its raw value
(per `row:raw()`), e.g. if `row.owner` refers to a row on the user table, then
`row.owner` will retrieve that user as a row object, whereas `row._owner` will
just retrieve that user's primary key.

### Row Members

#### row.entity
The entity object which the row belongs to.

#### row:\_debug()
Returns the row's inner data for debugging purposes.

#### row:delete()
Marks the row for deletion, and prevents further access. Note that the row
isn't actually deleted on the database until the next row:flush() call is
made.

#### row:flush(skip\_fkeys) -> still\_dirty
Propagates any changes to the database. If `skip_fkeys` is true, then it will
attempt to null out any foreign keys that may not be present on the database
yet, where possible. If it does so, it will remain dirty even if it succeeds.

Returns true if it succeeds and the field is no longer dirty as a result;
otherwise, returns false.

#### row:fields() -> iterator
Acts as a pairs() equivalent for all the fields on the row, including those
currently set to nil. i.e.:

for field,value in row:fields() do print(field, value) end

#### row:get(field) -> value
Retrieve the value for a given field by name. If the field is a foreign key
field, this will retrieve the appropriate row object from the cache or
database as needed.

#### row:raw(field) -> raw\_value
Retrieve the raw value for a given field by name. If the field is a foreign
key field, this will return the actual value stored on the database; thus,
the key itself will be returned, not the row object.

#### row:set(field, value)
Sets the field to a new value, marking the row as dirty in the process. Note
that changes aren't propogated to the database until the next successful
row:flush() call is made, either directly or via em.flush() or
the parent entity's entity:flush().
