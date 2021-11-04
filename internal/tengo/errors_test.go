package tengo

import (
	"errors"
	"fmt"
	"testing"
)

func (s TengoIntegrationSuite) TestIsDatabaseError(t *testing.T) {
	err1 := errors.New("non-db error")
	if IsDatabaseError(err1) {
		t.Errorf("IsDatabaseError unexpectedly returned true for non-database error type=%T", err1)
	}
	_, err2 := s.d.Connect("doesnt_exist", "")
	if !IsDatabaseError(err2) {
		t.Errorf("IsDatabaseError unexpectedly returned false for error of type=%T", err2)
	}
}

func (s TengoIntegrationSuite) TestIsSyntaxError(t *testing.T) {
	err := errors.New("non-db error")
	if IsSyntaxError(err) {
		t.Errorf("IsSyntaxError unexpectedly returned true for non-database error type=%T", err)
	}

	db, err := s.d.ConnectionPool("testing", "")
	if err != nil {
		t.Fatalf("Unable to get connection")
	}
	_, err = db.Exec("ALTER TAABBEL actor ENGINE=InnoDB")
	if err == nil {
		t.Error("Bad syntax still returned nil error unexpectedly")

	} else if !IsSyntaxError(err) {
		t.Errorf("Error of type %T %+v unexpectedly not considered syntax error", err, err)
	}
	_, err = db.Exec("ALTER TABLE doesnt_exist ENGINE=InnoDB")
	if err == nil {
		t.Error("Bad alter still returned nil error unexpectedly")
	} else if IsSyntaxError(err) {
		t.Errorf("Error of type %T %+v unexpectedly considered syntax error", err, err)
	}
}

func (s TengoIntegrationSuite) TestIsAccessError(t *testing.T) {
	err := errors.New("non-db error")
	if IsAccessError(err) {
		t.Errorf("IsAccessError unexpectedly returned true for non-database error type=%T", err)
	}

	// Hack username in DSN to no longer be correct
	inst := s.d.Instance
	inst.BaseDSN = fmt.Sprintf("badname%s", inst.BaseDSN)
	_, err = inst.ConnectionPool("", "")
	if err == nil {
		t.Error("ConnectionPool unexpectedly returned nil error")
	} else if !IsAccessError(err) {
		t.Errorf("Error of type %T %+v unexpectedly not considered access error", err, err)
	}
	inst.BaseDSN = inst.BaseDSN[7:]
	db, err := inst.ConnectionPool("testing", "")
	if err != nil {
		t.Errorf("ConnectionPool unexpectedly returned error: %s", err)
	}
	_, err = db.Exec("ALTER TABLE doesnt_exist ENGINE=InnoDB")
	if err == nil {
		t.Error("Bad alter still returned nil error unexpectedly")
	} else if IsAccessError(err) {
		t.Errorf("Error of type %T %+v unexpectedly considered access error", err, err)
	}
}
