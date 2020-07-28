package sett_test

import (
	"github.com/prasanthmj/sett"
	"os"
	"syreclabs.com/go/faker"
	"testing"
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
	err := s.Set(k, v)
	if err != nil {
		t.Error("Set operation failed:", err)
		return
	}
	vr, err := s.Get(k)
	if err != nil {
		t.Error("Get operation failed:", err)
		return
	}
	if vr != v {
		t.Error("Value does not match")
		return
	}

	v2 := faker.RandomString(12)
	err = s.Set(k, v2)
	if err != nil {
		t.Error("Set operation failed:", err)
		return
	}
	vr2, err := s.Get(k)
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

	err := s.Set(k, v)
	if err != nil {
		t.Error("Set operation failed:", err)
	}

	k2 := faker.RandomString(8)
	v2 := faker.RandomString(8)

	err = s.Set(k2, v2)
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
	_, err = s.Get(k)
	if err == nil {
		t.Error("Key \"key\" should not be found, but it was")
		return
	}

	vr2, err := s.Get(k2)
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

	err := s.Table(table).Set(k, v)
	if err != nil {
		t.Error("TableSet operation failed:", err)
		return
	}
	// should be able to retrieve the value of a key
	vr, err := s.Table(table).Get(k)
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

	err := s.Table(table).Set(k, v)
	if err != nil {
		t.Error("TableSet operation failed:", err)
	}
	// should be able to delete key from table
	if err := s.Table(table).Delete(k); err != nil {
		t.Error("Delete operation failed:", err)
	}

	// key should be gone
	_, err = s.Table(table).Get(k)
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

		err := s.Set(k, v)
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

		err := s.Set(k, v)
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

		err := s.Table(table).Set(k, v)
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
		s.Table(table).Set(k, v)
	}
	for i := 0; i < 15; i++ {
		k := "prefix_" + faker.RandomString(8)
		v := faker.RandomString(8)
		s.Table(table).Set(k, v)
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
		s.Table(table).Set(k, v)
		keys[i] = k
	}

	// should be able to delete "table"
	if err := s.Table(table).Drop(); err != nil {
		t.Error("Table Drop, unexpected error", err)
		return
	}

	for i := 0; i < 15; i++ {
		_, err := s.Table(table).Get(keys[i])

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

	err := s.Table(table).Set(k, v)
	if err != nil {
		t.Error("Error setting table value ", err)
		return
	}

	k2 := faker.RandomString(8)
	v2 := faker.RandomString(8)

	err = s.Set(k2, v2)
	if err != nil {
		t.Error("Error setting  value ", err)
		return
	}

	s.Table("another-table").Set(k, v)

	//m, _ := s.Scan()
	//t.Logf("map:\n%v", m)

	vr2, err := s.Get(k2)
	if err != nil {
		t.Error("Error getting value ", err)
		return
	}
	if vr2 != v2 {
		t.Error("Value does not match for key ", k2)
	}

}
