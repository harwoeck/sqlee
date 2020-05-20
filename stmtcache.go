package sqlee

import (
	"context"
	"database/sql"
	"sync"
)

// StmtCache caches prepared statements and offers it's functionality as a
// drop-in-replacement for the `*sql.DB` variable you would normally use.
// StmtCache can only be used for non-transactional queries.
type StmtCache struct {
	prep  preparer
	cache map[string]*sql.Stmt
	baton sync.RWMutex
}

var _ preparer = (*StmtCache)(nil) //*StmtCache implements preparer

// newStmtCache initializes a new StmtCache
func newStmtCache(prep preparer) *StmtCache {
	return &StmtCache{
		prep:  prep,
		cache: make(map[string]*sql.Stmt),
		baton: sync.RWMutex{},
	}
}

// PrepareContext will check if the query has already a cache prepared
// statement assigned and return it. When nothing as been cached yet, it will
// prepare the statement using the passed context and save it for later use.
func (sc *StmtCache) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	var stmt *sql.Stmt
	var ok bool

	// read in read-lock from cache
	func() {
		sc.baton.RLock()
		defer sc.baton.RUnlock()

		stmt, ok = sc.cache[query]
	}()
	if ok {
		return stmt, nil
	}

	// prepare stmt with context
	stmt, err := sc.prep.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	// write statement with write-lock to cache
	func() {
		sc.baton.Lock()
		defer sc.baton.Unlock()

		sc.cache[query] = stmt
	}()

	// return prepared statement
	return stmt, nil
}

// EvictAll removes and closes all currently cached prepared statements. When
// the Close function of a statement returns an error it is sent to the
// unbuffered error channel (Attention: this means you ned to receive on the
// channel. If you don't receive errors sent to it, the application will block)
//
// Example usage:
//     for err := sc.EvictAll() {
//         fmt.Println(err)
//     }
func (sc *StmtCache) EvictAll() chan error {
	errChan := make(chan error)

	go func() {
		defer close(errChan)

		sc.baton.Lock()
		defer sc.baton.Unlock()

		for key, stmt := range sc.cache {
			delete(sc.cache, key)

			err := stmt.Close()
			if err != nil {
				errChan <- err
			}
		}
	}()

	return errChan
}

// EvictAllDiscardErr removes and closes all currently cached prepared
// statements. When the close function of a statement returns an error it is
// discarded.
func (sc *StmtCache) EvictAllDiscardErr() {
	sc.baton.Lock()
	defer sc.baton.Unlock()

	for key, stmt := range sc.cache {
		delete(sc.cache, key)
		// discard error
		_ = stmt.Close()
	}
}
