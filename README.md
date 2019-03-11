# autoprepare
Automatically prepare frequently-executed `database/sql` statements

`autoprepare` is a small library that transparently monitors the most frequent `database/sql` queries that
your application executes and then automatically creates and uses the corresponding prepared statements.

Instead of doing (error handling omitted for brevity):

```
db, _ := sql.Open("mysql", "/mydb")
res, _ := db.QueryContext(context.Background(), "SELECT * FROM mytable WHERE id = ?", 1)
// ... many more queries ...
```

you can do:

```
db, _ := sql.Open("mysql", "/mydb")
dbsc, _ := autoprepare.New(db)
res, _ := dbsc.QueryContext(context.Background(), "SELECT * FROM mytable WHERE id = ?", 1)
// ... many more queries ...
```

and `autoprepare` will transparently start using prepared statements for the most common queries.

`autoprepare` is deliberately pretty conservative, as it will only prepare the most frequently executed 
statements, and it will only prepare a limited number of them (by default 16, see `WithMaxPreparedStmt`).
Statement preparation occurs in the background, not when queries are executed, to limit latency spikes
and to simplify the code. Statement preparation is triggered after a sizable amount of queries have been
sent (currently 5000), and will result in a single statement (the most common in the last 5000 queries)
being prepared. The frequency of executions is estimated using an exponential moving average.
If a prepared statement stops being frequently executed it will be closed so that other statements can be
prepared instead.

To limit the amount of memory used, both by the library and on the database, only statements shorter
than a certain length (by default 4KB, see `WithMaxQueryLen`) are eligibile for preparation.

It is recommended to not raise `WithMaxPreparedStmt` and to use `sql.(*DB).SetMaxConn` to set a limit to 
how many connections the `database/sql` pool will open to the database, as potentially every prepared
statement will need to be created on every connection, and some databases have internal limits to how
many prepared statements can exist at the same time: in this case the recommendation is to monitor the
number of prepared statements and memory usage on the database server.

It is important to understand that `autoprepare` uses the SQL query string to lookup prepared statements;
this means that it is critical, to allow `autoprepare` to be effective, to use placeholders in queries
(e.g. `SELECT name FROM t WHERE id = ?`). It is possible to not use placeholders, but in this case every
combination of arguments (e.g. `SELECT name FROM t WHERE id = 29` and `SELECT name FROM t WHERE id = 81`)
will be considered a different query for the purpose of measuring the most frequently-executed queries.

Also note that using multiple statements in the same query (e.g. `SELECT 1; SELECT 2`) may not be supported
by the underlying driver.

`autoprepare` has been tested with the `sqlite3` and `mysql` drivers, but should reasonably work with
every conformant `database/sql` driver.
