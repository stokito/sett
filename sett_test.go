package sett_test

import (
	"encoding/gob"
	"github.com/prasanthmj/sett"
	"os"
	"syreclabs.com/go/faker"
	"testing"
	"time"
)

// instance for testing
var s *sett.Sett

func TestMain(m *testing.M) {
	// set up database for tests

	s = sett.Open(sett.DefaultOptions("./data/jobsdb7"))
	defer s.Close()

	// clean up
	os.RemoveAll("./data/jobsdb7")

	os.Exit(m.Run())
}

func TestSet(t *testing.T) {
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

func TestScanFilter(t *testing.T) {
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

	scan, _ := s.Scan(prefix)
	l := len(scan)
	if l != 15 {
		t.Error("ScanAll expected 15 keys, got", l)
	}
}

func TestTableScanAll(t *testing.T) {
	table := faker.RandomString(8)
	for i := 0; i < 15; i++ {
		k := faker.RandomString(8)
		//Make sure the key is unique
		for c := 0; c < 100; c++ {
			if !s.Table(table).HasKey(k) {
				break
			}
		}
		v := faker.RandomString(8)

		err := s.Table(table).SetStr(k, v)
		if err != nil {
			t.Error("TableSet operation failed:", err)
			return
		}
	}
	scan, _ := s.Table(table).Scan()
	l := len(scan)
	if l != 15 {
		t.Error("ScanAll expected 5 keys, got", l)
	}

}

func TestTableScanFilter(t *testing.T) {
	table := faker.RandomString(8)
	for i := 0; i < 15; i++ {
		k := faker.RandomString(8)
		v := faker.RandomString(8)
		s.Table(table).SetStr(k, v)
	}
	for i := 0; i < 15; i++ {
		k := "prefix_" + faker.RandomString(8)
		v := faker.RandomString(8)
		s.Table(table).SetStr(k, v)
	}

	scan, _ := s.Table(table).Scan("prefix_")
	l := len(scan)
	if l != 15 {
		t.Error("TestTableScanFilter expected 15 keys, got", l)
	}
}

func TestDrop(t *testing.T) {
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

	//m, _ := s.Scan()
	//t.Logf("map:\n%v", m)

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
