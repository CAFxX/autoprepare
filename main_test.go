package autoprepare

import (
	"flag"
	"os"
	"testing"
)

var (
	SqliteDSN     = flag.String("sqlite", "file::memory:?mode=memory&cache=shared", "SQLite3 DSN")
	MysqlDSN      = flag.String("mysql", "", "MySQL DSN")
	BenchDriver   = flag.String("benchdriver", "sqlite3", "database/sql driver to use for benchmarking (sqlite3 or mysql)")
	BenchCompare  = flag.Bool("benchcompare", true, "Run benchmarks to compare performance of not using autoprepare")
	BenchParallel = flag.Bool("benchparallel", true, "Run the parallel benchmarks")
)

func BenchDB() (string, string) {
	switch *BenchDriver {
	case "sqlite3":
		return "sqlite3", *SqliteDSN
	case "mysql":
		return "mysql", *MysqlDSN
	default:
		panic("unknown driver")
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}
