package sett

import (
	"bytes"
	"encoding/gob"
	"errors"
	badger "github.com/dgraph-io/badger/v2"
	"log"
	"time"
)

var (
	DefaultOptions         = badger.DefaultOptions
	DefaultIteratorOptions = badger.DefaultIteratorOptions
)

type Sett struct {
	db        *badger.DB
	table     string
	ttl       time.Duration
	keyLength int
}

// Open is constructor function to create badger instance,
// configure defaults and return struct instance
func Open(opts badger.Options) *Sett {
	s := Sett{}

	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal("Open: create or open failed")
	}
	s.db = db
	return &s
}

// Table selects the table, operations are to be performed
// on. Used as a prefix on the keys passed to badger
func (s *Sett) Table(table string) *Sett {
	return &Sett{db: s.db, table: table}
}

//WithTTL sets a (TTL) Time To Live value for values in this table
// The TTL affects only the values added after the TTL is set.
// Not applied to the values added before
func (s *Sett) WithTTL(d time.Duration) *Sett {
	s.ttl = d
	return s
}

//WithKeyLength sets the key length for generated string keys
// for example with Insert() call where the key is generated
func (s *Sett) WithKeyLength(len int) *Sett {
	s.keyLength = len
	return s
}

type genericContainer struct {
	V interface{}
}

func (s *Sett) GetUniqueKey(len int) (string, error) {
	var key string
	var err error
	//We don't want to try indefinitely.
	for t := 0; t < 100; t++ {
		key, err = GenerateID(len)
		if err != nil {
			return "", err
		}
		if !s.HasKey(key) {
			return key, nil
		}
	}
	return "", errors.New("Couldn't generate a unique key ")
}

func (s *Sett) Insert(val interface{}) (string, error) {
	keylen := 22
	if s.keyLength > 0 {
		keylen = s.keyLength
	}
	key, err := s.GetUniqueKey(keylen)
	if err != nil {
		return "", err
	}
	err = s.SetStruct(key, val)
	if err != nil {
		return "", err
	}
	return key, nil
}

//SetStruct can be used to set the value as any struct type
func (s *Sett) SetStruct(key string, val interface{}) error {
	var bValue bytes.Buffer
	container := genericContainer{V: val}
	err := gob.NewEncoder(&bValue).Encode(&container)
	if err != nil {
		return err
	}
	err = s.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry([]byte(s.makeKey(key)), bValue.Bytes())
		if s.ttl > 0 {
			e.WithTTL(s.ttl)
		}
		err = txn.SetEntry(e)
		return err
	})
	return err
}

//Cut is to remove an item and return it
//This is to avoid first getting the item and then deleting later
//When you want to make sure there is only one owner to the
//item, use Cut
func (s *Sett) Cut(key string) (interface{}, error) {
	var err error
	var container genericContainer
	err = s.db.Update(func(txn *badger.Txn) error {
		bkey := []byte(s.makeKey(key))
		item, err := txn.Get(bkey)
		if err != nil {
			return err
		}
		var val []byte
		val, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		err = gob.NewDecoder(bytes.NewBuffer(val)).Decode(&container)
		if err != nil {
			return err
		}
		err = txn.Delete(bkey)
		if err != nil {
			return err
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return container.V, nil
}

func (s *Sett) GetStruct(key string) (interface{}, error) {

	var err error
	var container genericContainer
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(s.makeKey(key)))
		if err != nil {
			return err
		}
		var val []byte
		val, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		err = gob.NewDecoder(bytes.NewBuffer(val)).Decode(&container)
		return err
	})
	if err != nil {
		return nil, err
	}
	return container.V, nil
}

// Set passes a key & value to badger. Expects string for both
// key and value for convenience, unlike badger itself
func (s *Sett) SetStr(key string, val string) error {
	var err error
	err = s.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry([]byte(s.makeKey(key)), []byte(val))
		if s.ttl > 0 {
			e.WithTTL(s.ttl)
		}
		err = txn.SetEntry(e)
		return err
	})
	return err
}

// Get returns value of queried key from badger
func (s *Sett) GetStr(key string) (string, error) {
	var val []byte
	var err error
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(s.makeKey(key)))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	return string(val), err
}

func (s *Sett) Set(key string, val interface{}) error {
	switch val.(type) {
	case string:
		return s.SetStr(key, val.(string))
	default:
		return s.SetStruct(key, val)
	}
}

func (s *Sett) Get(key string) (interface{}, error) {
	ret, err := s.GetStruct(key)
	if err != nil {
		return s.GetStr(key)
	}
	return ret, err
}

//HasKey checks the existence of a key
func (s *Sett) HasKey(key string) bool {
	_, err := s.Get(key)
	if err != nil {
		return false
	}
	return true
}

// Keys returns all keys from a (virtual) table. An
// optional filter allows the table prefix on the key search
// to be expanded
func (s *Sett) Keys(filter ...string) ([]string, error) {
	var result []string
	var err error
	err = s.db.View(func(txn *badger.Txn) error {
		var fullFilter string
		it := txn.NewIterator(DefaultIteratorOptions)
		defer it.Close()

		if len(filter) > 1 {
			return errors.New("Can't accept more than one filters")
		}
		if len(s.table) > 0 {
			fullFilter = s.table + ":"
		}

		if len(filter) == 1 {
			fullFilter += filter[0]
		}
		tn := len(s.table + ":")

		for it.Seek([]byte(fullFilter)); it.ValidForPrefix([]byte(fullFilter)); it.Next() {
			item := it.Item()
			k := string(item.Key())
			k = k[tn:]

			result = append(result, k)
		}
		return err
	})
	return result, err
}

type FilterFunc func(k string, v interface{}) bool

func (s *Sett) Filter(filter FilterFunc) ([]string, error) {
	var result []string
	var err error
	err = s.db.View(func(txn *badger.Txn) error {
		var fullFilter string
		it := txn.NewIterator(DefaultIteratorOptions)
		defer it.Close()

		if len(s.table) > 0 {
			fullFilter = s.table
		}

		tn := len(s.table + ":")

		for it.Seek([]byte(fullFilter)); it.ValidForPrefix([]byte(fullFilter)); it.Next() {
			item := it.Item()
			k := string(item.Key())
			k = k[tn:]

			var container genericContainer
			var val []byte
			val, err = item.ValueCopy(nil)
			if err != nil {
				return err
			}
			err = gob.NewDecoder(bytes.NewBuffer(val)).Decode(&container)
			if err != nil {
				return err
			}
			if filter(k, container.V) {
				result = append(result, k)
			}

		}
		return err
	})
	return result, err
}

// Delete removes a key and its value from badger instance
func (s *Sett) Delete(key string) error {
	var err error
	err = s.db.Update(func(txn *badger.Txn) error {
		err = txn.Delete([]byte(s.makeKey(key)))
		return err
	})
	return err
}

// Drop removes all keys with table prefix from badger,
// the effect is as if a table was deleted
func (s *Sett) Drop() error {
	var err error
	var deleteKey []string
	err = s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(DefaultIteratorOptions)
		prefix := []byte(s.table)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := string(item.Key())
			deleteKey = append(deleteKey, key)
		}
		it.Close()
		return nil
	})
	err = s.db.Update(func(txn *badger.Txn) error {
		for _, d := range deleteKey {
			err = txn.Delete([]byte(d))
			if err != nil {
				break
			}
		}
		return err
	})
	return err
}

// Close wraps badger Close method for defer
func (s *Sett) Close() error {
	return s.db.Close()
}

func (s *Sett) makeKey(key string) string {
	// makes the real key to be stored which
	// comprises table name and key set
	if len(s.table) <= 0 {
		return key
	}
	return s.table + ":" + key
}
