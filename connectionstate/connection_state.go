// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package connectionstate

import (
	"strings"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Type represents the type of ConnectionState that is used.
// ConnectionState can be either transactional or non-transactional.
// Transactional ConnectionState means that any changes to the state
// during a transaction will only be persisted and visible after the
// transaction if the transaction is committed.
// Non-transactional ConnectionState means that changes that are made
// to the state during a transaction are persisted directly and will
// be visible after the transaction regardless whether the transaction
// was committed or rolled back.
type Type int

const (
	// TypeDefault indicates that the default ConnectionState type of the database dialect should be used.
	// GoogleSQL uses non-transactional ConnectionState by default.
	// PostgreSQL uses transactional ConnectionState by default.
	TypeDefault Type = iota
	// TypeTransactional ConnectionState means that changes to the state during a transaction will only be
	// persisted and visible after the transaction if the transaction is committed.
	TypeTransactional
	// TypeNonTransactional ConnectionState means that changes to the state during a transaction are persisted
	// directly and remain visible after the transaction, regardless whether the transaction committed or not.
	TypeNonTransactional
)

// ConnectionState contains connection the state of a connection in a map.
type ConnectionState struct {
	connectionStateType   Type
	inTransaction         bool
	properties            map[string]ConnectionPropertyValue
	transactionProperties map[string]ConnectionPropertyValue
	localProperties       map[string]ConnectionPropertyValue
}

// ExtractValues extracts a map of ConnectionPropertyValue from a map of strings.
// The converter function that is registered for the corresponding ConnectionProperty
// is used to convert the string value to the actual value.
func ExtractValues(properties map[string]ConnectionProperty, values map[string]string) (map[string]ConnectionPropertyValue, error) {
	result := make(map[string]ConnectionPropertyValue)
	for _, prop := range properties {
		value, err := extractValue(prop, values)
		if err != nil {
			return nil, err
		}
		if value != nil {
			result[prop.Key()] = value
		}
	}
	return result, nil
}

func extractValue(prop ConnectionProperty, values map[string]string) (ConnectionPropertyValue, error) {
	strVal, ok := values[prop.Key()]
	if !ok {
		strVal, ok = values[strings.Replace(prop.Key(), "_", "", -1)]
		if !ok {
			// No value found.
			return nil, nil
		}
	}
	val, err := prop.Convert(strVal)
	if err != nil {
		return nil, err
	}
	return prop.CreateInitialValue(val)
}

// NewConnectionState creates a new ConnectionState instance with the given initial values.
// The Type must be either TypeTransactional or TypeNonTransactional.
func NewConnectionState(connectionStateType Type, properties map[string]ConnectionProperty, initialValues map[string]ConnectionPropertyValue) (*ConnectionState, error) {
	if connectionStateType == TypeDefault {
		return nil, status.Error(codes.InvalidArgument, "connection state type cannot be TypeDefault")
	}
	state := &ConnectionState{
		connectionStateType:   connectionStateType,
		properties:            make(map[string]ConnectionPropertyValue),
		transactionProperties: nil,
		localProperties:       nil,
	}
	for key, value := range initialValues {
		state.properties[key] = value.Copy()
	}
	for key, value := range properties {
		if _, ok := state.properties[key]; !ok {
			state.properties[key] = value.CreateDefaultValue()
		}
	}
	return state, nil
}

// Begin starts a new transaction for this ConnectionState.
func (cs *ConnectionState) Begin() error {
	if cs.inTransaction {
		return status.Error(codes.FailedPrecondition, "connection state is already in transaction")
	}
	cs.inTransaction = true
	return nil
}

// Commit the current ConnectionState.
// This resets all local property values to the value they had before the transaction.
// If the Type is TypeTransactional, then all pending state changes are committed.
// If the Type is TypeNonTransactional, all state changes during the transaction have already been persisted during the
// transaction.
func (cs *ConnectionState) Commit() error {
	if !cs.inTransaction {
		return status.Error(codes.FailedPrecondition, "connection state is not in a transaction")
	}
	cs.inTransaction = false
	if cs.transactionProperties != nil {
		for key, value := range cs.transactionProperties {
			cs.properties[key] = value
		}
	}
	cs.transactionProperties = nil
	cs.localProperties = nil
	return nil
}

// Rollback the current transactional state.
// This resets all local property values to the value they had before the transaction.
// If the Type is TypeTransactional, then all pending state changes are rolled back.
// If the Type is TypeNonTransactional, all state changes during the transaction have already been persisted during the
// transaction.
func (cs *ConnectionState) Rollback() error {
	if !cs.inTransaction {
		return status.Error(codes.FailedPrecondition, "connection state is not in a transaction")
	}
	cs.inTransaction = false
	cs.transactionProperties = nil
	cs.localProperties = nil
	return nil
}

// Reset the state to the initial values. Only the properties with a Context equal to or higher
// than the given Context will be reset. E.g. if the given Context is ContextUser, then properties
// with ContextStartup will not be reset.
func (cs *ConnectionState) Reset(context Context) error {
	cs.transactionProperties = nil
	cs.localProperties = nil
	var remove map[string]bool
	for _, value := range cs.properties {
		if value.ConnectionProperty().Context() >= context {
			if value.RemoveAtReset() {
				if remove == nil {
					remove = make(map[string]bool)
				}
				remove[value.ConnectionProperty().Key()] = true
			} else {
				if err := value.ResetValue(context); err != nil {
					return err
				}
			}
		}
	}
	for key := range remove {
		delete(cs.properties, key)
	}
	return nil
}

func (cs *ConnectionState) value(property ConnectionProperty, returnErrForUnknownProperty bool) (ConnectionPropertyValue, error) {
	if val, ok := cs.localProperties[property.Key()]; ok {
		return val, nil
	}
	if val, ok := cs.transactionProperties[property.Key()]; ok {
		return val, nil
	}
	if val, ok := cs.properties[property.Key()]; ok {
		return val, nil
	}
	if returnErrForUnknownProperty {
		return nil, status.Errorf(codes.InvalidArgument, "unrecognized configuration property %q", property.Key())
	}
	return nil, nil
}
