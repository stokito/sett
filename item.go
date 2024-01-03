package sett

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	badger "github.com/dgraph-io/badger/v2"
)

const (
	STRUCT_TYPE = 1
	STRING_TYPE = 2
)

type SettItem struct {
	fullKey string
	s       *Sett
	txn     *badger.Txn
	unlock  bool
}

type SettValueItem struct {
	V      interface{}
	Locked bool
}

func NewSettItem(s *Sett, txn *badger.Txn, key string) *SettItem {
	k := s.makeKey(key)
	return &SettItem{fullKey: k, s: s, txn: txn, unlock: false}
}

func (si *SettItem) Unlock(u bool) {
	si.unlock = u
}

func (si *SettItem) GetStructValue() (*SettValueItem, error) {
	item, err := si.txn.Get([]byte(si.fullKey))
	if err != nil {
		return nil, err
	}
	meta := item.UserMeta()
	if (meta & 0x0F) != STRUCT_TYPE {
		return nil, errors.New("Attempt to fetch Struct where item was not struct type")
	}
	var val []byte
	val, err = item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	var container genericContainer
	err = gob.NewDecoder(bytes.NewBuffer(val)).Decode(&container)
	if err != nil {
		return nil, err
	}
	var locked bool = false
	if (meta & 0x80) != 0 {
		locked = true
	}
	ret := &SettValueItem{V: container.V, Locked: locked}
	return ret, nil
}

func (si *SettItem) IsLocked() bool {
	item, err := si.txn.Get([]byte(si.fullKey))
	if err != nil {
		return false
	}
	if (item.UserMeta() & 0x80) != 0 {
		return true
	}
	return false
}

func (si *SettItem) Lock() error {
	item, err := si.txn.Get([]byte(si.fullKey))
	if err != nil {
		return err
	}
	meta := item.UserMeta()
	if (meta & 0x80) != 0 {
		return fmt.Errorf("The item was already locked")
	}
	var val []byte
	val, err = item.ValueCopy(nil)
	if err != nil {
		return err
	}
	e := badger.NewEntry([]byte(si.fullKey), val)
	meta = meta | 0x80
	err = si.setEntry(e, meta)
	return err
}

func (si *SettItem) SetStructValue(val interface{}) error {
	if !si.unlock && si.IsLocked() {
		return fmt.Errorf("The item with key %s is locked. Can't update now", si.fullKey)
	}
	var bValue bytes.Buffer
	container := genericContainer{V: val}
	err := gob.NewEncoder(&bValue).Encode(&container)
	if err != nil {
		return err
	}
	e := badger.NewEntry([]byte(si.fullKey), bValue.Bytes())

	err = si.setEntry(e, STRUCT_TYPE)
	return err
}

func (si *SettItem) setEntry(e *badger.Entry, vtype byte) error {
	if si.s.ttl > 0 {
		e.WithTTL(si.s.ttl)
	}
	e.WithMeta(vtype)
	return si.txn.SetEntry(e)
}

func (si *SettItem) SetStringValue(val string) error {
	if !si.unlock && si.IsLocked() {
		return fmt.Errorf("The item with key %s is locked. Can't update now", si.fullKey)
	}
	e := badger.NewEntry([]byte(si.fullKey), []byte(val))

	err := si.setEntry(e, STRING_TYPE)
	return err
}

func (si *SettItem) GetStringValue() (string, error) {
	item, err := si.txn.Get([]byte(si.fullKey))
	if err != nil {
		return "", err
	}
	meta := item.UserMeta()
	if (meta & 0x0F) != STRING_TYPE {
		return "", errors.New("Attempt to fetch Struct where item was not struct type")
	}
	var val []byte
	val, err = item.ValueCopy(nil)
	if err != nil {
		return "", err
	}
	return string(val), nil
}

func (si *SettItem) Delete() error {
	if !si.unlock && si.IsLocked() {
		return fmt.Errorf("The item with key %s is locked. Can't delete now", si.fullKey)
	}

	return si.txn.Delete([]byte(si.fullKey))
}
