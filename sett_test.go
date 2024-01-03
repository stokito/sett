package sett_test

import (
	"encoding/gob"
	"errors"
	"github.com/prasanthmj/sett/v2"
	"go.uber.org/goleak"
	"os"
	"sync"
	"syreclabs.com/go/faker"
	"testing"
	"time"
)

func initSett() *sett.Sett {
	os.RemoveAll("./data/jobsdb7")
	opts := sett.DefaultOptions("./data/jobsdb7")
	opts.Logger = nil
	s, _ := sett.Open(opts)
	return s
}

func closeSet(s *sett.Sett) {
	s.Close()
	// clean up
	os.RemoveAll("./data/jobsdb7")
}

func TestSet(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
	)

	s := initSett()
	defer closeSet(s)

	k := faker.RandomString(8)
	v := faker.RandomString(8)
	// should be able to add a key and value
	err := s.SetStr(k, v)
	if err != nil {
		t.Error("Set operation failed:", err)
		return
	}
	vr, err := s.GetStr(k)
	if err != nil {
		t.Error("Get operation failed:", err)
		return
	}
	if vr != v {
		t.Error("Value does not match")
		return
	}

	v2 := faker.RandomString(12)
	err = s.SetStr(k, v2)
	if err != nil {
		t.Error("Set operation failed:", err)
		return
	}
	vr2, err := s.GetStr(k)
	if err != nil {
		t.Error("Get operation failed:", err)
		return
	}
	if vr2 != v2 {
		t.Error("Second retrieval Value does not match")
		return
	}

}

func TestDelete(t *testing.T) {
	s := initSett()
	defer closeSet(s)
	k := faker.RandomString(8)
	v := faker.RandomString(8)

	err := s.SetStr(k, v)
	if err != nil {
		t.Error("Set operation failed:", err)
	}

	k2 := faker.RandomString(8)
	v2 := faker.RandomString(8)

	err = s.SetStr(k2, v2)
	if err != nil {
		t.Error("Set operation failed:", err)
		return
	}

	// should be able to delete key
	if err := s.Delete(k); err != nil {
		t.Error("Delete operation failed:", err)
		return
	}

	// key should be gone
	_, err = s.GetStr(k)
	if err == nil {
		t.Error("Key \"key\" should not be found, but it was")
		return
	}

	vr2, err := s.GetStr(k2)
	if err != nil {
		t.Error("Error getting second key", err)
		return
	}
	if vr2 != v2 {
		t.Error("Error second value does not match", err)
		return
	}

}

func TestTableGet(t *testing.T) {
	s := initSett()
	defer closeSet(s)

	table := faker.RandomString(8)
	k := faker.RandomString(8)
	v := faker.RandomString(8)

	err := s.Table(table).SetStr(k, v)
	if err != nil {
		t.Error("TableSet operation failed:", err)
		return
	}
	// should be able to retrieve the value of a key
	vr, err := s.Table(table).GetStr(k)
	if err != nil {
		t.Error("TableGet operation failed:", err)
		return
	}
	// val should be "value"
	if v != vr {
		t.Errorf("TableGet: Expected value %s, got %s", v, vr)
	}
}

func TestTableDelete(t *testing.T) {
	s := initSett()
	defer closeSet(s)

	table := faker.RandomString(8)
	k := faker.RandomString(8)
	v := faker.RandomString(8)

	err := s.Table(table).SetStr(k, v)
	if err != nil {
		t.Error("TableSet operation failed:", err)
	}
	// should be able to delete key from table
	if err := s.Table(table).Delete(k); err != nil {
		t.Error("Delete operation failed:", err)
	}

	// key should be gone
	_, err = s.Table(table).GetStr(k)
	if err == nil {
		t.Error("Key in table \"table\" should not be found, but it was")
	}
}

func TestKeysFilter(t *testing.T) {
	s := initSett()
	defer closeSet(s)

	//Add some random key values first
	for i := 0; i < 15; i++ {
		k := faker.RandomString(12)
		//Make sure the key is unique
		for c := 0; c < 100; c++ {
			if !s.HasKey(k) {
				break
			}
		}
		v := faker.RandomString(22)

		err := s.SetStr(k, v)
		if err != nil {
			t.Error("Set operation failed:", err)
			return
		}
	}

	//Add some keys with specific prefix
	prefix := "prefix_"
	for i := 0; i < 15; i++ {
		k := prefix + faker.RandomString(8)
		//Make sure the key is unique
		for c := 0; c < 100; c++ {
			if !s.HasKey(k) {
				break
			}
		}
		v := faker.RandomString(8)

		err := s.SetStr(k, v)
		if err != nil {
			t.Error("Set operation failed:", err)
			return
		}
	}

	keys, _ := s.Keys(prefix)
	l := len(keys)
	if l != 15 {
		t.Error("Keys expected 15 keys, got", l)
	}
}

func TestDrop(t *testing.T) {
	s := initSett()
	defer closeSet(s)

	table := faker.RandomString(8)
	var keys [15]string
	for i := 0; i < 15; i++ {
		k := faker.RandomString(8)
		v := faker.RandomString(8)
		s.Table(table).SetStr(k, v)
		keys[i] = k
	}

	// should be able to delete "table"
	if err := s.Table(table).Drop(); err != nil {
		t.Error("Table Drop, unexpected error", err)
		return
	}

	for i := 0; i < 15; i++ {
		_, err := s.Table(table).GetStr(keys[i])

		if err == nil {
			t.Errorf("Key %s in table \"batch\" should not be found as table droppped, but it was", keys[i])
		}
	}
	// check that a key should be gone
}

func TestTableNameShouldntPersist(t *testing.T) {
	s := initSett()
	defer closeSet(s)

	table := faker.RandomString(8)
	k := faker.RandomString(8)
	v := faker.RandomString(8)

	err := s.Table(table).SetStr(k, v)
	if err != nil {
		t.Error("Error setting table value ", err)
		return
	}

	k2 := faker.RandomString(8)
	v2 := faker.RandomString(8)

	err = s.SetStr(k2, v2)
	if err != nil {
		t.Error("Error setting  value ", err)
		return
	}

	s.Table("another-table").SetStr(k, v)

	vr2, err := s.GetStr(k2)
	if err != nil {
		t.Error("Error getting value ", err)
		return
	}
	if vr2 != v2 {
		t.Error("Value does not match for key ", k2)
	}

}

func TestTTL(t *testing.T) {
	s := initSett()
	defer closeSet(s)

	table := faker.RandomString(8)

	pk := faker.RandomString(8)
	pv := faker.RandomString(8)

	err := s.Table(table).SetStr(pk, pv)
	if err != nil {
		t.Error("Couldn't set key value", err)
		return
	}

	k := faker.RandomString(8)
	v := faker.RandomString(8)

	mytable := s.Table(table).WithTTL(100 * time.Millisecond)

	err = mytable.SetStr(k, v)
	if err != nil {
		t.Error("Couldn't set key value", err)
		return
	}

	time.Sleep(200 * time.Millisecond)

	_, err = mytable.GetStr(k)
	if err == nil {
		t.Error("Could fetch key value even after TTL expiry")
	}

	vpk, err := mytable.GetStr(pk)
	if err != nil {
		t.Error("Couldn't get key value for permanent key", err)
		return
	}
	if vpk != pv {
		t.Errorf("Couldn't get key value for permanent key. Expected %s Received %s", pv, vpk)
	}
}

type Signup struct {
	Name  string
	Email string
	Age   int
}

func TestSettingStruct(t *testing.T) {
	s := initSett()
	defer closeSet(s)

	gob.Register(&Signup{})
	var su Signup
	su.Name = faker.Name().Name()
	su.Email = faker.Internet().SafeEmail()
	su.Age = faker.Number().NumberInt(2)

	k := faker.RandomString(8)

	err := s.Table("signups").SetStruct(k, &su)
	if err != nil {
		t.Error("Error setting struct value ", err)
		return
	}

	sur, err := s.Table("signups").GetStruct(k)
	if err != nil {
		t.Error("Error getting struct value ", err)
	}

	sur2 := sur.(*Signup)

	if sur2.Name != su.Name {
		t.Errorf("The retrieved value does not match %s vs %s", sur2.Name, su.Name)
	}
}

func TestSimpleSet(t *testing.T) {
	s := initSett()
	defer closeSet(s)

	k := faker.RandomString(12)
	v := faker.RandomString(12)

	err := s.Set(k, v)
	if err != nil {
		t.Error("Set has thrown error ", err)
		return
	}
	vr, err := s.Get(k)
	if err != nil {
		t.Error("Get has thrown error", err)
		return
	}
	if vr != v {
		t.Errorf("The returned values does not match expected %s got %s", v, vr)
	}
}

type UserSession struct {
	ID    string
	Email string
}

func TestInsert(t *testing.T) {
	gob.Register(&UserSession{})
	s := initSett()
	defer closeSet(s)

	session := UserSession{}
	session.ID = faker.RandomString(12)
	session.Email = faker.Internet().Email()

	id, err := s.Table("sessions").Insert(&session)
	if err != nil {
		t.Error("Error inserting a value", err)
		return
	}
	t.Logf("Inserted session ID %s ID length %d", id, len(id))
	sessret, err := s.Table("sessions").GetStruct(id)
	if err != nil {
		t.Error("Error getting inserted value", err)
		return
	}
	session2 := sessret.(*UserSession)
	if session2.ID != session.ID || session2.Email != session.Email {
		t.Error("retrieved session value does not match")
		return
	}

	id, err = s.Table("sessions").WithKeyLength(8).Insert(&session)
	if err != nil {
		t.Error("Error inserting a value", err)
		return
	}
	if len(id) != 8 {
		t.Error("The Id length has no effect")
	}
	t.Logf("Inserted session ID %s ID length %d", id, len(id))

}

func TestInsertWithExpiry(t *testing.T) {
	gob.Register(&UserSession{})
	s := initSett()
	defer closeSet(s)

	session := UserSession{}
	session.ID = faker.RandomString(12)
	session.Email = faker.Internet().Email()

	id, err := s.Table("sessions").WithTTL(200 * time.Millisecond).Insert(&session)

	time.Sleep(300 * time.Millisecond)

	_, err = s.Table("sessions").GetStruct(id)
	if err == nil {
		t.Error("Expiry is not working for Insert")
	}
}

func TestGetKeys(t *testing.T) {
	gob.Register(&UserSession{})
	s := initSett()
	defer closeSet(s)

	table := faker.RandomString(12)

	for i := 0; i < 15; i++ {
		session := UserSession{}
		session.ID = faker.RandomString(12)
		session.Email = faker.Internet().Email()

		k, err := s.Table(table).Insert(&session)
		if err != nil {
			t.Error("Error inserting new items ", err)
			return
		}
		t.Logf("key: %s", k)
	}

	keys, err := s.Table(table).Keys()
	if err != nil {
		t.Error("Error Getting item keys ", err)
		return
	}
	if len(keys) != 15 {
		t.Errorf("Expected 15 keys got %d", len(keys))
	}
	//t.Logf("Received keys %v ", keys)
	for _, k := range keys {
		t.Logf("key %s ", k)
		it, err := s.Table(table).GetStruct(k)
		if err != nil {
			t.Errorf("Error getting item with key %s : %v ", k, err)
			return
		}
		sess := it.(*UserSession)
		t.Logf("retrieved session obj %v ", sess)
	}
}

func TestCutting(t *testing.T) {
	gob.Register(&Signup{})
	s := initSett()
	defer closeSet(s)

	var su Signup
	su.Name = faker.Name().Name()
	su.Email = faker.Internet().SafeEmail()
	su.Age = faker.Number().NumberInt(2)

	k := faker.RandomString(8)

	err := s.Table("signups").SetStruct(k, &su)
	if err != nil {
		t.Error("Error setting struct value ", err)
		return
	}
	sur, err := s.Table("signups").Cut(k)
	if err != nil {
		t.Error("Error cutting struct value ", err)
		return
	}

	sur2 := sur.(*Signup)

	if sur2.Name != su.Name {
		t.Errorf("The retrieved value does not match %s vs %s", sur2.Name, su.Name)
	}

	_, err = s.Table("signups").GetStruct(k)
	if err == nil {
		t.Error("The item can be retrieved even after cutting it")
	}
}

func TestCuttingWithInsert(t *testing.T) {
	gob.Register(&Signup{})
	s := initSett()
	defer closeSet(s)

	table := faker.RandomString(8)
	var su Signup
	su.Name = faker.Name().Name()
	su.Email = faker.Internet().SafeEmail()
	su.Age = faker.Number().NumberInt(2)

	k, err := s.Table(table).Insert(&su)
	if err != nil {
		t.Error("Error inserting struct value ", err)
		return
	}

	sur, err := s.Table(table).Cut(k)
	if err != nil {
		t.Error("Error cutting struct value ", err)
		return
	}

	sur2 := sur.(*Signup)

	if sur2.Name != su.Name {
		t.Errorf("The retrieved value does not match %s vs %s", sur2.Name, su.Name)
	}

	_, err = s.Table(table).GetStruct(k)
	if err == nil {
		t.Error("The item can be retrieved even after cutting it")
	}
}

type Item struct {
	Color string
	Name  string
}

func TestFilterFunc(t *testing.T) {
	gob.Register(&Item{})
	s := initSett()
	defer closeSet(s)

	table := faker.RandomString(8)
	var itm1 Item
	itm1.Color = "green"
	itm1.Name = faker.RandomString(12)
	s.Table(table).Insert(&itm1)

	var itm2 Item
	itm2.Color = "red"
	itm2.Name = faker.RandomString(12)
	s.Table(table).Insert(&itm2)

	var itm3 Item
	itm3.Color = "green"
	itm3.Name = faker.RandomString(12)
	s.Table(table).Insert(&itm3)

	keys, err := s.Table(table).Filter(func(k string, i interface{}) bool {
		it := i.(*Item)
		if it.Color == "red" {
			return true
		}
		return false
	})
	if err != nil {
		t.Errorf("Error running filter %v", err)
		return
	}
	if len(keys) != 1 {
		t.Errorf("Filter didn't find the right keys. got this: %v", keys)
		return
	}
	i2, err := s.Table(table).GetStruct(keys[0])
	if err != nil {
		t.Errorf("Error getting item %s ", keys[0])
		return
	}
	it2 := i2.(*Item)

	if it2.Name != itm2.Name {
		t.Errorf("Filter retrieval is incorrect expected %s received %s", itm2.Name, it2.Name)
	}
}

type TaskObj struct {
	ID     uint64
	Status string
	Access uint64
}
type ItemStatus struct {
	TaskID     uint64
	Accessed   uint64
	AccessedBy uint64
	Errors     []string
}

func TestUpdate(t *testing.T) {
	gob.Register(&TaskObj{})
	s := initSett()
	defer closeSet(s)

	table := faker.RandomString(8)
	var task TaskObj
	task.ID = uint64(faker.Number().NumberInt64(3))
	key, err := s.Table(table).Insert(&task)
	if err != nil {
		t.Errorf("Error inserting task %v", err)
		return
	}
	itask, err := s.Table(table).Update(key, func(iv interface{}) error {
		tobj := iv.(*TaskObj)
		tobj.Access += 1
		tobj.Status = "inprogress"
		return nil
	}, false)

	if err != nil {
		t.Errorf("Error updating task %v", err)
		return
	}

	tobj2 := itask.(*TaskObj)

	t.Logf("Received task obj after update ID %d Access %d Status %s ",
		tobj2.ID, tobj2.Access, tobj2.Status)
}

func TestConcurrentAccess(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
	)
	store := initSett()
	defer closeSet(store)

	var maxItems uint64 = 100
	var i uint64
	gob.Register(&TaskObj{})
	tab := faker.RandomString(8)
	stats := make(map[string]*ItemStatus)

	table := store.Table(tab)

	t.Logf("Creating Items ...")
	for i = 0; i < maxItems; i++ {
		t := &TaskObj{i, "", 0}
		k, err := table.Insert(t)
		if err != nil {
			continue
		}
		stats[k] = &ItemStatus{}
	}

	access_key := make(chan string)
	closed := make(chan struct{})
	var wg sync.WaitGroup
	var access sync.RWMutex

	t.Logf("Creating goroutines to access items ...")
	for m := 0; m < 10; m++ {
		go func() {
			wg.Add(1)
			defer wg.Done()
			for {
				select {
				case akey := <-access_key:
					t.Logf("to access key %s", akey)
					iobj, err := store.Table(tab).Cut(akey)
					t.Logf("Cut the item %s", akey)
					if err == nil {
						tobj := iobj.(*TaskObj)
						access.Lock()
						stats[akey].TaskID = tobj.ID
						stats[akey].Accessed += 1
						access.Unlock()
					} else {
						t.Logf("Couldn't cut item %s", akey)
					}
				case <-closed:
					return
				}
			}
		}()
	}

	t.Logf("Sending signals to access items ...")
	keys, err := table.Keys()
	if err != nil {
		t.Errorf("Error accessing keys of the table %v", err)
	} else {
		for _, km := range keys {
			t.Logf("Signalling access key (multiple times) %s", km)
			for kk := 0; kk < 10; kk++ {
				access_key <- km
			}

		}
	}

	<-time.After(4 * time.Second)
	close(closed)

	wg.Wait()

	t.Logf("Checking access counts ...")

	for _, ki := range keys {
		stat, ok := stats[ki]
		if !ok {
			t.Errorf("Key %s was not in the stats", ki)
			continue
		}
		t.Logf("Key %s Accessed %d Task %d", ki, stat.Accessed, stat.TaskID)
	}
}

func TestConcurrentUpdate(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
	)

	store := initSett()
	defer closeSet(store)

	var maxItems uint64 = 100
	var i uint64
	gob.Register(&TaskObj{})
	tab := faker.RandomString(8)
	stats := make(map[string]*ItemStatus)

	table := store.Table(tab)

	t.Logf("Creating Items ...")
	for i = 0; i < maxItems; i++ {
		t := &TaskObj{i, "", 0}
		k, err := table.Insert(t)
		if err != nil {
			continue
		}
		stats[k] = &ItemStatus{}
	}

	access_key := make(chan string, 5)
	closed := make(chan struct{})
	var wg sync.WaitGroup
	var access sync.RWMutex

	t.Logf("Creating goroutines to access items ...")
	for m := 0; m < 10; m++ {
		go func() {
			wg.Add(1)
			defer wg.Done()
			for {
				select {
				case akey := <-access_key:
					t.Logf("to access key %s", akey)
					iobj, err := store.Table(tab).Update(akey, func(iv interface{}) error {
						tobj := iv.(*TaskObj)
						if tobj.Status == "inprogress" {
							return errors.New("Conflicting Access")
						}
						tobj.Status = "inprogress"
						return nil
					}, false)
					if err == nil {
						tobj2 := iobj.(*TaskObj)
						access.Lock()
						stats[akey].TaskID = tobj2.ID
						stats[akey].Accessed += 1
						access.Unlock()
					} else {
						access.Lock()
						stats[akey].Errors = append(stats[akey].Errors, err.Error())
						access.Unlock()
					}
				case <-closed:
					return
				}
			}
		}()
	}

	t.Logf("Sending signals to access items ...")
	keys, err := table.Keys()
	if err != nil {
		t.Errorf("Error accessing keys of the table %v", err)
	} else {
		for _, km := range keys {
			t.Logf("Signalling access key (multiple times) %s", km)
			for kk := 0; kk < 10; kk++ {
				access_key <- km
			}

		}
	}

	<-time.After(4 * time.Second)
	close(closed)

	wg.Wait()

	t.Logf("Checking access counts ...")

	for _, ki := range keys {
		stat, ok := stats[ki]
		if !ok {
			t.Errorf("Key %s was not in the stats", ki)
			continue
		}
		t.Logf("Key %s Accessed %d Task %d", ki, stat.Accessed, stat.TaskID)
		if stat.Accessed > 1 {
			t.Errorf("Key %s got accessed more than once", ki)
		}
		/*for _, e := range stats[ki].Errors {
			t.Logf("Task %d Error: %s", stat.TaskID, e)
		}*/
	}
}

func TestLock(t *testing.T) {
	gob.Register(&TaskObj{})
	s := initSett()
	defer closeSet(s)

	table := faker.RandomString(8)
	var task TaskObj
	task.ID = uint64(faker.Number().NumberInt64(3))
	key, err := s.Table(table).Insert(&task)
	if err != nil {
		t.Errorf("Error inserting task %v", err)
		return
	}
	err = s.Table(table).Lock(key)
	if err != nil {
		t.Errorf("Couldn't lock item %s ", key)
		return
	}
	err = s.Table(table).Lock(key)
	if err == nil {
		t.Errorf("Could lock a locked item %s ", key)
		return
	} else {
		t.Logf("Correctly Can't lock a locked item err %v", err)
	}

	var task2 TaskObj
	err = s.Table(table).SetStruct(key, &task2)
	if err == nil {
		t.Errorf("Can set a locked item %s ", key)
	} else {
		t.Logf("Correctly Can't update a locked item err %v", err)
	}
	_, err = s.Table(table).Update(key, func(v interface{}) error {
		tobj := v.(*TaskObj)
		tobj.ID = uint64(faker.Number().NumberInt64(3))
		return nil
	}, false)

	if err == nil {
		t.Errorf("Can update a locked item without unlocking")
	} else {
		t.Logf("Correctly Can't update a locked item err %v", err)
	}

	it2, err := s.Table(table).Update(key, func(v interface{}) error {
		tobj := v.(*TaskObj)
		tobj.ID = uint64(faker.Number().NumberInt64(3))
		return nil
	}, true)
	if err != nil {
		t.Errorf("Couldn't update an item even with unlock option ")
	} else {
		tobj2 := it2.(*TaskObj)
		t.Logf("Updated after unlocking. Orig ID %d updated ID %d ", task.ID, tobj2.ID)
	}

}

func TestLockAndDelete(t *testing.T) {
	gob.Register(&TaskObj{})
	s := initSett()
	defer closeSet(s)

	table := faker.RandomString(8)
	var task TaskObj
	task.ID = uint64(faker.Number().NumberInt64(3))
	key, err := s.Table(table).Insert(&task)
	if err != nil {
		t.Errorf("Error inserting task %v", err)
		return
	}
	err = s.Table(table).Lock(key)
	if err != nil {
		t.Errorf("Couldn't lock item %s ", key)
		return
	}

	err = s.Table(table).Delete(key)
	if err == nil {
		t.Errorf("Can delete locked item %s", key)
	} else {
		t.Logf("Correctly can't delete locked item. %v", err)
	}

	err = s.Table(table).UnlockAndDelete(key)
	if err != nil {
		t.Errorf("Can't delete item even after unlocking %v", err)
	}

}
