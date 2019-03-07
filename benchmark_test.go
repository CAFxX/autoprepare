package autoprepare

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

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

	const warmup = 10000

	for _, c := range benchmarks {
		init := func() *sql.DB {
			db, err := sql.Open(BenchDB())
			if err != nil {
				panic(err)
			}

			_, err = db.Exec(c.create)
			if err != nil {
				panic(err)
			}

			return db
		}
		b.Run(c.name, func(b *testing.B) {
			b.Run("Serial", func(b *testing.B) {
				if *Compare {
					b.Run("NotPrepared", func(b *testing.B) {
						db := init()
						defer db.Close()

						ctx := context.Background()

						for i := 0; i < warmup; i++ {
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
					b.Run("Prepared", func(b *testing.B) {
						db := init()
						defer db.Close()

						ctx := context.Background()

						ps, err := db.PrepareContext(ctx, c.query)
						if err != nil {
							panic(err)
						}
						defer ps.Close()

						for i := 0; i < warmup; i++ {
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
				b.Run("AutoPrepared", func(b *testing.B) {
					db := init()
					defer db.Close()

					ctx := context.Background()

					dbsc, err := New(db)
					if err != nil {
						panic(err)
					}
					for i := 0; i < warmup; i++ {
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
			})
			for _, par := range []int{1, 10, 100} {
				b.Run(fmt.Sprintf("Parallel-%d", par), func(b *testing.B) {
					if *Compare {
						b.Run("NotPrepared", func(b *testing.B) {
							db := init()
							defer db.Close()

							ctx := context.Background()

							for i := 0; i < warmup; i++ {
								res, err := db.QueryContext(ctx, c.query, c.args...)
								if err != nil {
									panic(err)
								}
								res.Close()
							}

							b.SetParallelism(par)
							b.RunParallel(func(pb *testing.PB) {
								for pb.Next() {
									res, err := db.QueryContext(ctx, c.query, c.args...)
									if err != nil {
										panic(err)
									}
									res.Close()
								}
							})
						})
						b.Run("Prepared", func(b *testing.B) {
							db := init()
							defer db.Close()

							ctx := context.Background()

							ps, err := db.PrepareContext(ctx, c.query)
							if err != nil {
								panic(err)
							}
							defer ps.Close()

							for i := 0; i < warmup; i++ {
								res, err := ps.QueryContext(ctx, c.args...)
								if err != nil {
									panic(err)
								}
								res.Close()
							}

							b.SetParallelism(par)
							b.RunParallel(func(pb *testing.PB) {
								for pb.Next() {
									res, err := ps.QueryContext(ctx, c.args...)
									if err != nil {
										panic(err)
									}
									res.Close()
								}
							})
						})
					}
					b.Run("AutoPrepared", func(b *testing.B) {
						db := init()
						defer db.Close()

						ctx := context.Background()

						dbsc, err := New(db)
						if err != nil {
							panic(err)
						}
						for i := 0; i < warmup; i++ {
							res, err := dbsc.QueryContext(ctx, c.query, c.args...)
							if err != nil {
								panic(err)
							}
							res.Close()
						}

						b.SetParallelism(par)
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								res, err := dbsc.QueryContext(ctx, c.query, c.args...)
								if err != nil {
									panic(err)
								}
								res.Close()
							}
						})
					})
				})
			}
		})
	}
}
