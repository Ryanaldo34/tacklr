package durableobject

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/ryanaldo34/tacklr/session"
)

// What an object stores, by key prefix. Each key starts with its kind and
// the escaped session id, so the stores meet the session store contracts
// for any id. Sequence numbers are fixed-width hex, so keys sort in order.
const (
	kindMeta        = "meta"
	kindCarry       = "carry"
	kindJournal     = "journal"
	kindMailbox     = "mailbox"
	kindMailboxHead = "mailboxhead"
	kindSnapshot    = "snapshot"
	kindCheckpoint  = "checkpoint"
	kindBody        = "body"
	kindSecrets     = "secrets"
	kindEvent       = "event"
	kindEventHead   = "eventhead"
	kindChild       = "child"
	kindOutbox      = "outbox"
	kindCall        = "call"
	// kindWire holds the wire state a protocol server keeps with the
	// session (Ref.Wire).
	kindWire = "wire"
)

// statusKey holds what Status and Jobs answer, so a cold object answers
// without running its session.
const statusKey = "status"

// scanPage is how many entries one read of a scan returns.
const scanPage = 256

// key is kind's key for session id.
func key(kind string, id session.ID) string {
	return kind + "/" + url.PathEscape(string(id))
}

// prefix is the prefix of kind's entries for session id.
func prefix(kind string, id session.ID) string {
	return key(kind, id) + "/"
}

// seqKey is entry n of kind for session id.
func seqKey(kind string, id session.ID, n uint64) string {
	return prefix(kind, id) + fmt.Sprintf("%016x", n)
}

// seqOf is the sequence number at the end of key, written by seqKey.
func seqOf(key string) uint64 {
	n, _ := strconv.ParseUint(key[len(key)-16:], 16, 64)
	return n
}

// childKey records that session id started child.
func childKey(id, child session.ID) string {
	return prefix(kindChild, id) + url.PathEscape(string(child))
}

// put is an Op that writes value at key.
func put(key string, value []byte) Op { return Op{Key: key, Value: value} }

// del is an Op that deletes key.
func del(key string) Op { return Op{Key: key, Delete: true} }

// scanAll calls fn for every entry under prefix, in key order, reading
// scanPage entries at a time.
func scanAll(s Storage, prefix string, fn func(Entry) error) error {
	after := ""
	for {
		entries, err := s.Scan(prefix, after, scanPage)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if err := fn(e); err != nil {
				return err
			}
		}
		if len(entries) < scanPage {
			return nil
		}
		after = entries[len(entries)-1].Key
	}
}

// deleteAll is a delete of every entry under prefix.
func deleteAll(s Storage, prefix string) ([]Op, error) {
	var ops []Op
	err := scanAll(s, prefix, func(e Entry) error {
		ops = append(ops, del(e.Key))
		return nil
	})
	return ops, err
}
