package autoprepare

import (
	"context"
	"database/sql"
)

func Example() {
	// connect to your database/sql of choice
	db, err := sql.Open("sqlite3", "file::memory:?mode=memory&cache=shared")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// create a SqlStmtCache to automatically prepare your statements
	dbsc, err := New(db)
	if err != nil {
		panic(err)
	}
	defer dbsc.Close()

	// now, instead of querying the database like usual:
	//   res, err := db.QueryContext(context.Background(), "SELECT * FROM mytable WHERE id = ?", 1)
	// you do (notice we are using the SqlStmtCache):
	res, err := dbsc.QueryContext(context.Background(), "SELECT * FROM mytable WHERE id = ?", 1)
	if err != nil {
		panic(err)
	}
	defer res.Close()
}
