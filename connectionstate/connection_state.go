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
	connectionStateType       Type
	inTransaction             bool
	properties                map[string]ConnectionPropertyValue
	transactionProperties     map[string]ConnectionPropertyValue
	localProperties           map[string]ConnectionPropertyValue
	statementScopedProperties map[string]ConnectionPropertyValue
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
		connectionStateType:       connectionStateType,
		properties:                make(map[string]ConnectionPropertyValue),
		transactionProperties:     nil,
		localProperties:           nil,
		statementScopedProperties: nil,
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

func toKey(extension, name string) (key string) {
	if extension == "" {
		key = strings.ToLower(name)
	} else {
		key = strings.ToLower(extension) + "." + strings.ToLower(name)
	}
	return
}

func (cs *ConnectionState) GetValue(extension, name string) (any, bool, error) {
	prop, err := cs.findProperty(extension, name)
	if err != nil {
		return nil, false, err
	}
	return prop.GetValue(cs)
}

func (cs *ConnectionState) SetValue(extension, name, value string, context Context) error {
	return cs.setValue(extension, name, value, context, valueTypeSession)
}

func (cs *ConnectionState) SetLocalValue(extension, name, value string, isSetTransaction bool) error {
	if isSetTransaction && !cs.inTransaction {
		return status.Error(codes.FailedPrecondition, "SET TRANSACTION can only be used in transaction blocks")
	}
	return cs.setValue(extension, name, value, ContextUser, valueTypeLocal)
}

func (cs *ConnectionState) SetStatementScopedValue(extension, name, value string) error {
	return cs.setValue(extension, name, value, ContextUser, valueTypeStatementScoped)
}

type valueType int

const (
	valueTypeSession = valueType(iota)
	valueTypeLocal
	valueTypeStatementScoped
)

var errInvalidValueType = status.Error(codes.InvalidArgument, "invalid value type")

func (cs *ConnectionState) setValue(extension, name, value string, context Context, valueType valueType) error {
	prop, err := cs.findProperty(extension, name)
	if err != nil {
		return err
	}
	// The special value null (or NULL) is used to set a connection property to the null value of the property (the
	// property's default value).
	// This is different from RESET, which will set the connection property to the value that it had when the
	// connection was created.
	if strings.EqualFold(strings.TrimSpace(value), "null") {
		switch valueType {
		case valueTypeSession:
			return prop.SetDefaultValue(cs, context)
		case valueTypeLocal:
			return prop.SetLocalDefaultValue(cs)
		case valueTypeStatementScoped:
			return prop.SetStatementScopedDefaultValue(cs)
		default:
			return errInvalidValueType
		}
	}

	// The special value default (or DEFAULT) is used to reset a connection property to its original value.
	if strings.EqualFold(strings.TrimSpace(value), "default") {
		switch valueType {
		case valueTypeSession:
			return prop.ResetValue(cs, context)
		case valueTypeLocal:
			return prop.ResetLocalValue(cs)
		case valueTypeStatementScoped:
			return prop.ResetStatementScopedValue(cs)
		default:
			return errInvalidValueType
		}
	}
	convertedValue, err := prop.Convert(value)
	if err != nil {
		return err
	}
	switch valueType {
	case valueTypeSession:
		return prop.SetUntypedValue(cs, convertedValue, context)
	case valueTypeLocal:
		return prop.SetLocalUntypedValue(cs, convertedValue)
	case valueTypeStatementScoped:
		return prop.SetStatementScopedUntypedValue(cs, convertedValue)
	default:
		return errInvalidValueType
	}

}

func (cs *ConnectionState) findProperty(extension, name string) (ConnectionProperty, error) {
	key := toKey(extension, name)
	var prop ConnectionProperty
	existingValue, ok := cs.properties[key]
	if !ok {
		prop = &TypedConnectionProperty[string]{
			key:       key,
			extension: extension,
			name:      name,
			context:   ContextUser,
			converter: ConvertString,
		}
		if extension == "" {
			return nil, unknownPropertyErr(prop)
		}
	} else {
		prop = existingValue.ConnectionProperty()
	}
	return prop, nil
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
			if value.isRemoved() {
				delete(cs.properties, key)
			} else {
				cs.properties[key] = value
			}
		}
	}
	cs.transactionProperties = nil
	cs.localProperties = nil
	cs.statementScopedProperties = nil
	for key, value := range cs.properties {
		if value.isRemoved() {
			delete(cs.properties, key)
		}
	}
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
	cs.statementScopedProperties = nil
	return nil
}

// ClearStatementScopedValues clears all values that have been set for the current statement.
// This function must be called after the execution of a statement that set statement values has finished.
func (cs *ConnectionState) ClearStatementScopedValues() {
	cs.statementScopedProperties = nil
}

// Reset the state to the initial values. Only the properties with a Context equal to or higher
// than the given Context will be reset. E.g. if the given Context is ContextUser, then properties
// with ContextStartup will not be reset.
func (cs *ConnectionState) Reset(context Context) error {
	cs.transactionProperties = nil
	cs.localProperties = nil
	cs.statementScopedProperties = nil
	for _, value := range cs.properties {
		if value.ConnectionProperty().Context() >= context {
			if value.RemoveAtReset() {
				delete(cs.properties, value.ConnectionProperty().Key())
			} else {
				if err := value.ResetValue(context); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (cs *ConnectionState) value(property ConnectionProperty, returnErrForUnknownProperty bool) (ConnectionPropertyValue, error) {
	if val, ok := cs.statementScopedProperties[property.Key()]; ok {
		return valOrError(property, returnErrForUnknownProperty, val)
	}
	if val, ok := cs.localProperties[property.Key()]; ok {
		return valOrError(property, returnErrForUnknownProperty, val)
	}
	if val, ok := cs.transactionProperties[property.Key()]; ok {
		return valOrError(property, returnErrForUnknownProperty, val)
	}
	if val, ok := cs.properties[property.Key()]; ok {
		return valOrError(property, returnErrForUnknownProperty, val)
	}
	if returnErrForUnknownProperty {
		return nil, unrecognizedPropertyErr(property)
	}
	return nil, nil
}

func valOrError(property ConnectionProperty, returnErrForUnknownProperty bool, val ConnectionPropertyValue) (ConnectionPropertyValue, error) {
	if returnErrForUnknownProperty && val.isRemoved() {
		return nil, unknownPropertyErr(property)
	}
	return val, nil
}

func unrecognizedPropertyErr(property ConnectionProperty) error {
	return status.Errorf(codes.InvalidArgument, "unrecognized configuration property %q", property.Key())
}
