# Sett2
Fork of https://github.com/olliephillips/sett 


## A golang package which offers a simple abstraction on BadgerDB key/value store

## API 

Creating or opening a store with Sett is identical to BadgerDB

```
s := sett.Open(sett.DefaultOptions("./data/mydb"))
defer s.Close()
```

Simple set, get and delete a key. Strings used in preference to byte slices. 

```
s.SetStr("hello", "world")
s.GetStr("hello")
s.Delete("hello")
```

### Tables

Tables are virtual, simply a prefix on the key, but formalised through the Sett API. The aim is to make organisation, reasoning, and usage, a little simpler.

Add a key/value to "client" table

```
s.Table("client").SetStr("hello", "world")
```

Get the value of a key from "client" table

```
s.Table("client").GetStr("hello")
```

Delete key and value from "client" table

```
s.Table("client").Delete("hello")
```

Drop "client" table including all keys

```
s.Table("client").Drop()
```

### TTL (Time to Live)

```
s.Table("session).WithTTL(1 * time.Hour).SetStr("hash", hash)
```

The key expires after 1 hour

## Custom Structs

```
type Signup struct {
	Name  string
	Email string
}
//You have to register the struct type with gob.Register()
//https://golang.org/pkg/encoding/gob/#Register

gob.Register(&Signup{})

s.Table("signups").SetStruct(email, &su)

uret, err := s.Table("signups").GetStruct(email)

user := uret.(*Signup)

```

## Insert
Insert is useful when you don't have a key but want to generate it.
For example, user sessions. You want a session ID in exchange for a struct. Use the Insert() function

```
gob.Register(&UserSession{})

session_key, err := s.Table("sessions").Insert(session)
//save the session_key in the cookie
```

You can set the Key length like so:
```
session_key, err := s.Table("sessions").WithKeyLength(12).Insert(session)
```
This can be combined with TTL (Time to live) as well

```
session_key, err := s.Table("sessions").WithTTL(1* time.Hour).Insert(session)
```

### Get entire table, or subset of table

Use `sett.Scan()` to return contents of a virtual table or a subset of that table based on a prefix filter.

Retrieving all key/values from the "client" table

```
scan, _ := s.Table("client").Scan()
for k, v := range scan {
	log.Println(k, v)
}
```

Using a prefix filter to get a subset of key/values from "client" table. In the below example the key prefix filter is "active_"

```
scan, _ := s.Table("client").Scan("active_")
for k, v := range scan {
	log.Println(k, v)
}
```