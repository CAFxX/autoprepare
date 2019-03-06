package sqlstmtcache

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestSqlStmtCache(t *testing.T) {
	db, err := sql.Open("sqlite3", "file::memory:?mode=memory&cache=shared")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE tables (a INT, b TEXT)")
	if err != nil {
		panic(err)
	}

	dbsc, err := New(db)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	for i := 0; i < 100000; i++ {
		res, err := dbsc.QueryContext(ctx, "SELECT * FROM tables")
		if err != nil {
			panic(err)
		}
		res.Close()
	}
}

func TestSqlStmtCachePollute(t *testing.T) {
	db, err := sql.Open("sqlite3", "file::memory:?mode=memory&cache=shared")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE tables (a INT, b TEXT)")
	if err != nil {
		panic(err)
	}

	dbsc, err := New(db)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	for i := 0; i < 500000; i++ {
		var a int
		if i%2 == 0 {
			a = int(math.Abs(rand.NormFloat64()*float64(dbsc.maxPS))) + (i / 10000)
		} else {
			a = rand.Intn(1 << 20)
		}
		res, err := dbsc.QueryContext(ctx, fmt.Sprintf("SELECT * FROM tables WHERE a = %d", a))
		if err != nil {
			panic(err)
		}
		res.Close()
	}

	expstmt := make(map[string]struct{})
	for i := uint32(0); i < dbsc.maxPS; i++ {
		expstmt[fmt.Sprintf("SELECT * FROM tables WHERE a = %d", i+49)] = struct{}{}
	}

	dbsc.l.RLock()
	defer dbsc.l.RUnlock()

	psCount := uint32(0)
	for _, s := range dbsc.stmt {
		if s.get() != nil {
			psCount++
		}
		_, expected := expstmt[s.q]
		if s.get() != nil && !expected {
			t.Errorf("unexpected prepared statement %q", s.q)
		} else if s.get() == nil && expected {
			t.Errorf("missing prepared statement %q", s.q)
		}
	}
	if len(dbsc.stmt) > dbsc.maxStmt {
		t.Errorf("too many statements: %d/%d", len(dbsc.stmt), dbsc.maxStmt)
	}

	psc := atomic.LoadUint32(&dbsc.psCount)
	if psc > dbsc.maxPS {
		t.Errorf("too many prepared statements: %d/%d", psc, dbsc.maxPS)
	}
	if psc != psCount {
		t.Errorf("inconsistent number of prepared statements: count %d, in map %d", psc, psCount)
	}
	if psc < dbsc.maxPS {
		t.Errorf("not enough prepared statements: %d/%d", psc, dbsc.maxPS)
	}
}

func BenchmarkSqlite(b *testing.B) {
	benchmarks := []struct {
		name   string
		create string
		query  string
		args   []interface{}
	}{
		{
			name:   "Select",
			create: "CREATE TABLE t (a INT, b TEXT); INSERT INTO t VALUES (1, \"hello\")",
			query:  "SELECT * FROM t LIMIT 1",
			args:   nil,
		},
		{
			name:   "Select1",
			create: "CREATE TABLE t (a INT, b TEXT); INSERT INTO t VALUES (1, \"hello\")",
			query:  "SELECT * FROM t WHERE a = ? LIMIT 1",
			args:   []interface{}{1},
		},
		{
			name:   "Insert2",
			create: "CREATE TABLE t (a INT, b TEXT)",
			query:  "INSERT INTO t (a, b) VALUES (?, ?)",
			args:   []interface{}{1, "hello"},
		},
		{
			name:   "Update2",
			create: "CREATE TABLE t (a INT, b TEXT); INSERT INTO t VALUES (1, \"hello\")",
			query:  "UPDATE t SET b = ? WHERE a = ?",
			args:   []interface{}{"hello", 1},
		},
	}

	for _, c := range benchmarks {
		init := func() *sql.DB {
			db, err := sql.Open("sqlite3", "file::memory:?mode=memory&cache=shared")
			if err != nil {
				panic(err)
			}

			_, err = db.Exec(c.create)
			if err != nil {
				panic(err)
			}

			return db
		}
		b.Run(c.name+"/NoCache", func(b *testing.B) {
			db := init()
			defer db.Close()

			ctx := context.Background()

			for i := 0; i < 10000; i++ {
				res, err := db.QueryContext(ctx, c.query, c.args...)
				if err != nil {
					panic(err)
				}
				res.Close()
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				res, err := db.QueryContext(ctx, c.query, c.args...)
				if err != nil {
					panic(err)
				}
				res.Close()
			}
		})
		b.Run(c.name+"/Cache", func(b *testing.B) {
			db := init()
			defer db.Close()

			ctx := context.Background()

			dbsc, err := New(db)
			if err != nil {
				panic(err)
			}
			for i := 0; i < 10000; i++ {
				res, err := dbsc.QueryContext(ctx, c.query, c.args...)
				if err != nil {
					panic(err)
				}
				res.Close()
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				res, err := dbsc.QueryContext(ctx, c.query, c.args...)
				if err != nil {
					panic(err)
				}
				res.Close()
			}
		})
		b.Run(c.name+"/Prepared", func(b *testing.B) {
			db := init()
			defer db.Close()

			ctx := context.Background()

			ps, err := db.PrepareContext(ctx, c.query)
			if err != nil {
				panic(err)
			}
			defer ps.Close()

			for i := 0; i < 10000; i++ {
				res, err := ps.QueryContext(ctx, c.args...)
				if err != nil {
					panic(err)
				}
				res.Close()
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				res, err := ps.QueryContext(ctx, c.args...)
				if err != nil {
					panic(err)
				}
				res.Close()
			}
		})
	}

}
