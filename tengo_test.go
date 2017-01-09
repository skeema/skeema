package tengo

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func primaryKey(cols ...*Column) *Index {
	return &Index{
		Name:       "PRIMARY",
		Columns:    cols,
		SubParts:   make([]uint16, len(cols)),
		PrimaryKey: true,
		Unique:     true,
	}
}

func aTable(nextAutoInc uint64) Table {
	columns := []*Column{
		&Column{
			Name:          "actor_id",
			TypeInDB:      "smallint(5) unsigned",
			AutoIncrement: true,
			Default:       ColumnDefaultNull,
		},
		&Column{
			Name:     "first_name",
			TypeInDB: "varchar(45)",
			Default:  ColumnDefaultNull,
		},
		&Column{
			Name:     "last_name",
			Nullable: true,
			TypeInDB: "varchar(45)",
			Default:  ColumnDefaultNull,
		},
		&Column{
			Name:     "last_update",
			TypeInDB: "timestamp(2)",
			Default:  ColumnDefaultExpression("CURRENT_TIMESTAMP(2)"),
			Extra:    "ON UPDATE CURRENT_TIMESTAMP(2)",
		},
		&Column{
			Name:     "ssn",
			TypeInDB: "char(10)",
			Default:  ColumnDefaultNull,
		},
		&Column{
			Name:     "alive",
			TypeInDB: "tinyint(1)",
			Default:  ColumnDefaultValue("1"),
		},
	}
	secondaryIndexes := []*Index{
		&Index{
			Name:     "idx_ssn",
			Columns:  []*Column{columns[4]},
			SubParts: []uint16{0},
			Unique:   true,
		},
		&Index{
			Name:     "idx_actor_name",
			Columns:  []*Column{columns[2], columns[1]},
			SubParts: []uint16{10, 1},
		},
	}

	var autoIncClause string
	if nextAutoInc > 1 {
		autoIncClause = fmt.Sprintf(" AUTO_INCREMENT=%d", nextAutoInc)
	}
	stmt := fmt.Sprintf(`CREATE TABLE `+"`"+`actor`+"`"+` (
  `+"`"+`actor_id`+"`"+` smallint(5) unsigned NOT NULL AUTO_INCREMENT,
  `+"`"+`first_name`+"`"+` varchar(45) NOT NULL,
  `+"`"+`last_name`+"`"+` varchar(45) DEFAULT NULL,
  `+"`"+`last_update`+"`"+` timestamp(2) NOT NULL DEFAULT CURRENT_TIMESTAMP(2) ON UPDATE CURRENT_TIMESTAMP(2),
  `+"`"+`ssn`+"`"+` char(10) NOT NULL,
  `+"`"+`alive`+"`"+` tinyint(1) NOT NULL DEFAULT '1',
  PRIMARY KEY (`+"`"+`actor_id`+"`"+`),
  UNIQUE KEY `+"`"+`idx_ssn`+"`"+` (`+"`"+`ssn`+"`"+`),
  KEY `+"`"+`idx_actor_name`+"`"+` (`+"`"+`last_name`+"`"+`(10),`+"`"+`first_name`+"`"+`(1))
) ENGINE=InnoDB%s DEFAULT CHARSET=utf8`, autoIncClause)

	return Table{
		Name:              "actor",
		Engine:            "InnoDB",
		CharSet:           "utf8",
		Columns:           columns,
		PrimaryKey:        primaryKey(columns[0]),
		SecondaryIndexes:  secondaryIndexes,
		NextAutoIncrement: nextAutoInc,
		createStatement:   stmt,
	}
}

func anotherTable() Table {
	columns := []*Column{
		&Column{
			Name:     "actor_id",
			TypeInDB: "smallint(5) unsigned",
			Default:  ColumnDefaultNull,
		},
		&Column{
			Name:     "film_name",
			TypeInDB: "varchar(60)",
			Default:  ColumnDefaultNull,
		},
	}
	secondaryIndex := &Index{
		Name:     "film_name",
		Columns:  []*Column{columns[1]},
		SubParts: []uint16{0},
	}
	stmt := `CREATE TABLE ` + "`" + `actor_in_film` + "`" + ` (
  ` + "`" + `actor_id` + "`" + ` smallint(5) unsigned NOT NULL,
  ` + "`" + `film_name` + "`" + ` varchar(60) NOT NULL,
  PRIMARY KEY (` + "`" + `actor_id` + "`" + `,` + "`" + `film_name` + "`" + `),
  KEY ` + "`" + `film_name` + "`" + ` (` + "`" + `film_name` + "`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=latin1`
	return Table{
		Name:             "actor_in_film",
		Engine:           "InnoDB",
		CharSet:          "latin1",
		Columns:          columns,
		PrimaryKey:       primaryKey(columns[0], columns[1]),
		SecondaryIndexes: []*Index{secondaryIndex},
		createStatement:  stmt,
	}
}

func unsupportedTable() Table {
	t := anotherTable()
	t.Name += "_with_fk"
	t.createStatement = `CREATE TABLE ` + "`" + `actor_in_film_with_fk` + "`" + ` (
  ` + "`" + `actor_id` + "`" + ` smallint(5) unsigned NOT NULL,
  ` + "`" + `film_name` + "`" + ` varchar(60) NOT NULL,
  PRIMARY KEY (` + "`" + `actor_id` + "`" + `,` + "`" + `film_name` + "`" + `),
  KEY ` + "`" + `film_name` + "`" + ` (` + "`" + `film_name` + "`" + `),
  CONSTRAINT ` + "`" + `fk_actor_id` + "`" + ` FOREIGN KEY (` + "`" + `actor_id` + "`" + `) REFERENCES ` + "`" + `actor` + "`" + ` (` + "`" + `actor_id` + "`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=latin1`
	t.UnsupportedDDL = true
	return t
}

func aSchema(name string, tables ...*Table) Schema {
	if tables == nil {
		tables = []*Table{}
	}
	s := Schema{
		Name:      name,
		CharSet:   "latin1",
		Collation: "latin1_swedish_ci",
		tables:    tables,
	}
	return s
}
