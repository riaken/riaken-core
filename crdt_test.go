package riaken_core

import (
	"testing"
)

func TestCrdtCounter(t *testing.T) {
	client := dial()
	defer client.Close()
	session := client.Session()
	defer session.Release()

	// Create a new Counter
	bucket := session.GetBucket("crdt_counter").Type("test_counters")
	counter := bucket.Crdt("foo").NewCounter()
	counter.Increment(1)
	if _, err := counter.Commit(); err != nil {
		t.Fatal(err.Error())
	}

	// Fetch without considering type
	crdt := bucket.Crdt("foo")
	if _, err := crdt.Fetch(); err != nil {
		t.Fatal(err.Error())
	}

	if crdt.Counter == nil {
		t.Fatal("exepected Counter to exist")
	}

	if crdt.Counter.Value != 1 {
		t.Errorf("expected: %d, got: %d", 1, crdt.Counter.Value)
	}

	// Increment
	crdt.Counter.Increment(5)
	if _, err := crdt.Counter.Commit(); err != nil {
		t.Fatal(err.Error())
	}

	// Refetch with new object
	crdt = bucket.Crdt("foo")
	if _, err := crdt.Fetch(); err != nil {
		t.Fatal(err.Error())
	}

	if crdt.Counter == nil {
		t.Fatal("exepected Counter to exist")
	}

	if crdt.Counter.Value != 6 {
		t.Errorf("expected: %d, got: %d", 6, crdt.Counter.Value)
	}

	// Decrement
	crdt.Counter.Decrement(3)
	if _, err := crdt.Counter.Commit(); err != nil {
		t.Fatal(err.Error())
	}

	if crdt.Counter.Value != 3 {
		t.Errorf("expected: %d, got: %d", 3, crdt.Counter.Value)
	}

	// Delete with standard object delete
	object := bucket.Object("foo")
	if _, err := object.Delete(); err != nil {
		t.Fatal(err.Error())
	}
}

func TestCrdtSet(t *testing.T) {
	client := dial()
	defer client.Close()
	session := client.Session()
	defer session.Release()

	// Create a new Set
	bucket := session.GetBucket("crdt_set").Type("test_sets")
	set := bucket.Crdt("foo").NewSet()
	set.Add("bar")
	set.Add("baz")
	if _, err := set.Commit(); err != nil {
		t.Fatal(err.Error())
	}

	// Fetch without considering type
	crdt := bucket.Crdt("foo")
	if _, err := crdt.Fetch(); err != nil {
		t.Fatal(err.Error())
	}

	if crdt.Set == nil {
		t.Fatal("exepected Set to exist")
	}

	if len(crdt.Set.Values) != 2 {
		t.Fatal("exepected 2 values")
	}

	crdt.Set.Add("aaa")
	crdt.Set.Add("bbb")
	crdt.Set.Remove("baz")

	if _, err := crdt.Set.Commit(); err != nil {
		t.Fatal(err.Error())
	}

	crdt = bucket.Crdt("foo")
	if _, err := crdt.Fetch(); err != nil {
		t.Fatal(err.Error())
	}

	if len(crdt.Set.Values) != 3 {
		t.Fatal("exepected 3 values")
	}

	// Delete with standard object delete
	object := bucket.Object("foo")
	if _, err := object.Delete(); err != nil {
		t.Fatal(err.Error())
	}
}

func TestCrdtMap(t *testing.T) {
	client := dial()
	defer client.Close()
	session := client.Session()
	defer session.Release()

	// Create a new Map
	bucket := session.GetBucket("crdt_map").Type("test_maps")
	crdt := bucket.Crdt("foo")
	mp := crdt.NewMap()
	// Add Flags to the Map
	mp.Flags["f1"] = true
	mp.Flags["f2"] = false
	// Add Registers to the Map
	mp.Registers["r1"] = "1r"
	mp.Registers["r2"] = "2r"
	// Add Counters to the Map
	mp.Counters["c1"] = crdt.NewCounter()
	mp.Counters["c1"].Increment(10)
	// Add Sets to the Map
	mp.Sets["s1"] = crdt.NewSet()
	mp.Sets["s1"].Add("1")
	mp.Sets["s1"].Add("2")
	mp.Sets["s1"].Add("3")
	mp.Sets["s2"] = crdt.NewSet()
	mp.Sets["s2"].Add("a")
	mp.Sets["s2"].Add("b")
	// Add Maps to the Map
	mp.Maps["m1"] = crdt.NewMap()
	mp.Maps["m1"].Flags["ff1"] = true
	mp.Maps["m1"].Registers["rr1"] = "1rr"
	mp.Maps["m1"].Counters["cc1"] = crdt.NewCounter()
	mp.Maps["m1"].Counters["cc1"].Increment(20)
	mp.Maps["m1"].Sets["ss1"] = crdt.NewSet()
	mp.Maps["m1"].Sets["ss1"].Add("111")
	mp.Maps["m1"].Sets["ss1"].Add("222")
	mp.Maps["m1"].Maps["mm1"] = crdt.NewMap()
	mp.Maps["m1"].Maps["mm1"].Flags["fff1"] = false
	mp.Maps["m1"].Maps["mm2"] = crdt.NewMap()
	mp.Maps["m1"].Maps["mm2"].Registers["123"] = "abc"
	if _, err := mp.Commit(); err != nil {
		t.Fatal(err.Error())
	}

	// Fetch without considering type
	crdt = bucket.Crdt("foo")
	if _, err := crdt.Fetch(); err != nil {
		t.Fatal(err.Error())
	}

	if crdt.Map == nil {
		t.Fatal("exepected Map to exist")
	}

	if v, ok := crdt.Map.Flags["f1"]; !ok {
		t.Error("expected map->flag->f1 to exist")
	} else {
		if !v {
			t.Error("flag should be true")
		}
	}

	if v, ok := crdt.Map.Registers["r1"]; !ok {
		t.Error("expected map->registers->r1 to exist")
	} else {
		if v != "1r" {
			t.Errorf("expected: %s, got: %s", "1r", v)
		}
	}

	if v, ok := crdt.Map.Counters["c1"]; !ok {
		t.Error("expected map->counters->c1 to exist")
	} else {
		if v.Value != 10 {
			t.Errorf("expected: %d, got: %d", 10, v.Value)
		}
	}

	if v, ok := crdt.Map.Sets["s1"]; !ok {
		t.Error("expected map->sets->s1 to exist")
	} else {
		if len(v.Values) != 3 {
			t.Errorf("expected: %d, got: %d", 3, len(v.Values))
		}
	}

	if v, ok := crdt.Map.Maps["m1"]; !ok {
		t.Error("expected map->maps->m1 to exist")
	} else {
		if b, ok := v.Flags["ff1"]; !ok {
			t.Error("expected map->maps->m1->flags->ff1 to exist")
		} else if !b {
			t.Error("flag should be true")
		}
	}

	if m1, ok := crdt.Map.Maps["m1"]; !ok {
		t.Error("expected map->maps->m1 to exist")
	} else {
		if m2, ok := m1.Maps["mm2"]; !ok {
			t.Error("expected map->maps->mm2 to exist")
		} else {
			if r, ok := m2.Registers["123"]; !ok {
				t.Error("expected map->maps->mm2->registers->123 to exist")
			} else {
				if r != "abc" {
					t.Error("register should be abc")
				}
			}
		}
	}

	// Change Values
	crdt.Map.Flags["f1"] = false
	crdt.Map.Remove(CRDT_MAP_REGISTER, "r2")
	crdt.Map.Counters["c1"].Decrement(6)
	crdt.Map.Sets["s1"].Remove("1")
	crdt.Map.Maps["m1"].Registers["rr1"] = "rr1rr"
	crdt.Map.Maps["m1"].Remove(CRDT_MAP_MAP, "mm2")
	if _, err := crdt.Map.Commit(); err != nil {
		t.Fatal(err.Error())
	}

	// Fetch without considering type
	crdt = bucket.Crdt("foo")
	if _, err := crdt.Fetch(); err != nil {
		t.Fatal(err.Error())
	}

	if crdt.Map == nil {
		t.Fatal("exepected Map to exist")
	}

	if v, ok := crdt.Map.Flags["f1"]; !ok {
		t.Error("expected map->flag->f1 to exist")
	} else {
		if v {
			t.Error("flag should be false")
		}
	}

	if _, ok := crdt.Map.Registers["r2"]; ok {
		t.Error("expected map->registers->r2 to not exist")
	}

	if v, ok := crdt.Map.Counters["c1"]; !ok {
		t.Error("expected map->counters->c1 to exist")
	} else {
		if v.Value != 4 {
			t.Errorf("expected: %d, got: %d", 4, v.Value)
		}
	}

	if v, ok := crdt.Map.Sets["s1"]; !ok {
		t.Error("expected map->sets->s1 to exist")
	} else {
		if len(v.Values) != 2 {
			t.Errorf("expected: %d, got: %d", 2, len(v.Values))
		}
	}

	if v, ok := crdt.Map.Maps["m1"]; !ok {
		t.Error("expected map->maps->m1 to exist")
	} else {
		if r, ok := v.Registers["rr1"]; !ok {
			t.Error("expected map->maps->m1->registers->rr1 to exist")
		} else if r != "rr1rr" {
			t.Errorf("expected: %s, got: %s", "rr1rr", r)
		}
	}

	if m1, ok := crdt.Map.Maps["m1"]; !ok {
		t.Error("expected map->maps->m1 to exist")
	} else {
		if _, ok := m1.Maps["mm2"]; ok {
			t.Error("map->maps->m1->maps->mm2 should not exist")
		}
	}

	// Delete with standard object delete
	object := bucket.Object("foo")
	if _, err := object.Delete(); err != nil {
		t.Fatal(err.Error())
	}
}
