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
	kindOutcome     = "outcome"
	kindSecrets     = "secrets"
	kindEvent       = "event"
	kindEventHead   = "eventhead"
	kindChild       = "child"
)

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

// deleteAll is a delete of every entry under prefix.
func deleteAll(s Storage, prefix string) ([]Op, error) {
	entries, err := s.Scan(prefix, "", 0)
	if err != nil {
		return nil, err
	}
	ops := make([]Op, len(entries))
	for i, e := range entries {
		ops[i] = del(e.Key)
	}
	return ops, nil
}
