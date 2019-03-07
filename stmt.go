package autoprepare

import (
	"database/sql"
	"runtime"
	"sync/atomic"
)

type stmt struct {
	ps        atomic.Value // *sql.Stmt
	psHandles uint32       // number of goroutines using ps
	hit       uint32
	q         string
}

// Helpers for atomic access to prepared statement

var noPS = sql.Stmt{}

func (s *stmt) acquire() *sql.Stmt {
	if s == nil {
		return nil
	}
	atomic.AddUint32(&s.psHandles, 1)
	if ps := s.get(); ps != nil {
		return ps
	}
	s.release()
	return nil
}

func (s *stmt) release() {
	atomic.AddUint32(&s.psHandles, ^uint32(0) /* -1 */)
}

func (s *stmt) wait() {
	for atomic.LoadUint32(&s.psHandles) > 0 {
		runtime.Gosched()
	}
}

func (s *stmt) get() *sql.Stmt {
	if s == nil {
		return nil
	}
	v, _ := s.ps.Load().(*sql.Stmt)
	if v == &noPS {
		return nil
	}
	return v
}

func (s *stmt) put(v *sql.Stmt) {
	if v == nil {
		v = &noPS
	}
	s.ps.Store(v)
}
