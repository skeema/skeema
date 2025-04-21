package tengo

import (
	"testing"
)

func (s TengoIntegrationSuite) TestCharacterSetsForFlavor(t *testing.T) {
	db, err := s.d.CachedConnectionPool("", "")
	if err != nil {
		t.Fatalf("Unable to connect to database: %v", err)
	}
	rows, err := db.Query("SELECT character_set_name, default_collate_name, maxlen FROM information_schema.character_sets")
	if err != nil {
		t.Fatalf("Unable to query database: %v", err)
	}
	defer rows.Close()

	csm := characterSetsForFlavor(s.d.Flavor())
	seen := make(map[string]bool, len(csm))

	for rows.Next() {
		var cs CharacterSet
		if err := rows.Scan(&cs.Name, &cs.DefaultCollation, &cs.MaxLength); err != nil {
			t.Fatalf("Unable to scan row: %v", err)
		}
		if expected, ok := csm[cs.Name]; !ok {
			t.Errorf("Flavor %s information_schema.character_sets has unexpected row %+v", s.d.Flavor(), cs)
			continue
		} else if expected != cs {
			t.Errorf("Flavor %s mismatch between information_schema.character_sets row %+v vs expected row %+v", s.d.Flavor(), cs, expected)
		}
		seen[cs.Name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Unable to iterate over result set: %v", err)
	}
	if len(seen) != len(csm) {
		for name := range csm {
			if !seen[name] {
				t.Errorf("Flavor %s does NOT have an information_schema.character_sets row for %s", s.d.Flavor(), name)
			}
		}
	}
}
