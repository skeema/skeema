package tengo

import (
	"errors"
	"strings"
	"testing"
)

func (s TengoIntegrationSuite) TestIsDatabaseError(t *testing.T) {
	err1 := errors.New("non-db error")
	if IsDatabaseError(err1) {
		t.Errorf("IsDatabaseError unexpectedly returned true for non-database error type=%T", err1)
	}
	_, err2 := s.d.ConnectionPool("doesnt_exist", "")
	if !IsDatabaseError(err2) {
		t.Errorf("IsDatabaseError unexpectedly returned false for error of type=%T", err2)
	}
}

func (s TengoIntegrationSuite) TestDatabaseErrorTypeFunctions(t *testing.T) {
	// Test IsSyntaxError
	err := errors.New("non-db error")
	if IsSyntaxError(err) {
		t.Errorf("IsSyntaxError unexpectedly returned true for non-database error type=%T", err)
	}
	db, err := s.d.CachedConnectionPool("", "")
	if err != nil {
		t.Fatalf("Unable to get connection")
	}
	_, syntaxErr := db.Exec("ALTER TAABBEL testing.actor ENGINE=InnoDB")
	if syntaxErr == nil {
		t.Error("Bad syntax still returned nil error unexpectedly")
	} else if !IsSyntaxError(syntaxErr) {
		t.Errorf("Error of type %T %+v unexpectedly not considered syntax error", syntaxErr, syntaxErr)
	}
	_, doesntExistErr := db.Exec("ALTER TABLE testing.doesnt_exist ENGINE=InnoDB")
	if doesntExistErr == nil {
		t.Error("Bad alter still returned nil error unexpectedly")
	} else if IsSyntaxError(doesntExistErr) {
		t.Errorf("Error of type %T %+v unexpectedly considered syntax error", doesntExistErr, doesntExistErr)
	}

	// Test IsObjectNotFoundError
	if !IsObjectNotFoundError(doesntExistErr) {
		t.Errorf("Error of type %T %+v unexpectedly not considered not-found error", doesntExistErr, doesntExistErr)
	}
	if IsObjectNotFoundError(syntaxErr) {
		t.Errorf("Error of type %T %+v unexpectedly considered not-found error", syntaxErr, syntaxErr)
	}

	// Test IsSessionVarNameError and IsSessionVarValueError
	_, invalidVarNameErr := s.d.ConnectionPool("", "invalidvar='hello'")
	_, globalOnlyVarNameErr := s.d.ConnectionPool("", "concurrent_insert=1")
	_, readOnlyVarNameErr := s.d.ConnectionPool("", "version_comment='hello'")
	_, invalidVarValueErr := s.d.ConnectionPool("", "sql_mode='superduperdb'")
	_, invalidVarValTypeErr := s.d.ConnectionPool("", "wait_timeout='hello'")
	if !IsSessionVarNameError(invalidVarNameErr) {
		t.Errorf("Incorrect behavior of IsSessionVarNameError: expected true for %T %+v, but false was returned", invalidVarNameErr, invalidVarNameErr)
	}
	if !IsSessionVarNameError(globalOnlyVarNameErr) {
		t.Errorf("Incorrect behavior of IsSessionVarNameError: expected true for %T %+v, but false was returned", globalOnlyVarNameErr, globalOnlyVarNameErr)
	}
	if !IsSessionVarNameError(readOnlyVarNameErr) {
		t.Errorf("Incorrect behavior of IsSessionVarNameError: expected true for %T %+v, but false was returned", readOnlyVarNameErr, readOnlyVarNameErr)
	}
	if IsSessionVarNameError(invalidVarValueErr) || IsSessionVarNameError(invalidVarValTypeErr) {
		t.Error("Incorrect behavior of IsSessionVarNameError: expected false, but true was returned")
	}
	if !IsSessionVarValueError(invalidVarValueErr) {
		t.Errorf("Incorrect behavior of IsSessionVarValueError: expected true for %T %+v, but false was returned", invalidVarValueErr, invalidVarValueErr)
	}
	if !IsSessionVarValueError(invalidVarValTypeErr) {
		t.Errorf("Incorrect behavior of IsSessionVarValueError: expected true for %T %+v, but false was returned", invalidVarValTypeErr, invalidVarValTypeErr)
	}
	if IsSessionVarValueError(invalidVarNameErr) || IsSessionVarValueError(globalOnlyVarNameErr) || IsSessionVarValueError(readOnlyVarNameErr) {
		t.Error("Incorrect behavior of IsSessionVarValueError: expected false, but true was returned")
	}

	// Test IsAccessDeniedError
	// Hack username in DSN to no longer be correct
	s.d.Instance.BaseDSN = strings.Replace(s.d.Instance.BaseDSN, s.d.Instance.Password, "wrongpw", 1)
	_, accessDeniedErr := s.d.Instance.ConnectionPool("", "")
	if !IsAccessDeniedError(accessDeniedErr) {
		t.Errorf("Error of type %T %+v unexpectedly not considered access denied error", accessDeniedErr, accessDeniedErr)
	}
	s.d.Instance.BaseDSN = strings.Replace(s.d.Instance.BaseDSN, "wrongpw", s.d.Instance.Password, 1)
	if IsAccessDeniedError(doesntExistErr) {
		t.Errorf("Error of type %T %+v unexpectedly considered access denied error", doesntExistErr, doesntExistErr)
	}
	if IsAccessPrivilegeError(accessDeniedErr) {
		t.Error("Incorrect behavior of IsAccessPrivilegeError")
	}
}
