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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Context indicates when a ConnectionProperty may be set.
type Context int

const (
	// ContextStartup is used for ConnectionProperty instances that may only be set at startup and may not be changed
	// during the lifetime of a connection.
	ContextStartup = iota
	// ContextUser is used for ConnectionProperty instances that may be set both at startup and during the lifetime of
	// a connection.
	ContextUser
)

func (c Context) String() string {
	switch c {
	case ContextStartup:
		return "Startup"
	case ContextUser:
		return "User"
	default:
		return "Unknown"
	}
}

// ConnectionProperty defines the public interface for connection properties.
type ConnectionProperty interface {
	// Key returns the unique key of the ConnectionProperty. This is equal to the name of the property for properties
	// without an extension, and equal to `extension.name` for properties with an extension.
	Key() string
	// Context returns the Context where the property is allowed to be updated (e.g. only at startup or during the
	// lifetime of a connection).
	Context() Context
	// CreateDefaultValue creates an initial value of the property with the default value of the property as the current
	// and reset value.
	CreateDefaultValue() ConnectionPropertyValue
	// CreateInitialValue creates an initial value of the property with the given value as the default and reset value.
	CreateInitialValue(value any) (ConnectionPropertyValue, error)
	// GetValue returns the current value of the property.
	// The returned bool indicates whether a value has been set or not. This can be used to distinguish between a
	// connection where the zero value has explicitly been set as the value and just having the default value.
	GetValue(state *ConnectionState) (any, bool, error)
	// SetUntypedValue sets a new value for the property without knowing the type in advance.
	// This is a synonym for SetValue.
	SetUntypedValue(state *ConnectionState, value any, context Context) error
	// SetLocalUntypedValue sets a new local value for the property without knowing the type in advance.
	// This is a synonym for SetLocalValue.
	SetLocalUntypedValue(state *ConnectionState, value any) error
	// SetDefaultValue sets the value of the property to the default value.
	// This is different from resetting the value, which sets it to the value that the property had when the connection
	// was created.
	SetDefaultValue(state *ConnectionState, context Context) error
	// SetLocalDefaultValue sets the local value of the property to the default value.
	// This is different from resetting the value, which sets it to the value that the property had when the connection
	// was created.
	SetLocalDefaultValue(state *ConnectionState) error
	// ResetValue sets the value of the property to its initial value.
	ResetValue(state *ConnectionState, context Context) error
	// ResetLocalValue sets the local value of the property to its initial value.
	ResetLocalValue(state *ConnectionState) error
	// Convert converts a string to the corresponding value type of the connection property.
	Convert(value string) (any, error)
}

// CreateConnectionProperty is used to create a new ConnectionProperty with a specific type. This function is intended
// for use by driver implementations at initialization time to define the properties that the driver supports.
func CreateConnectionProperty[T comparable](name, description string, defaultValue T, hasDefaultValue bool, validValues []T, context Context, converter func(value string) (T, error)) *TypedConnectionProperty[T] {
	return CreateConnectionPropertyWithExtension("", name, description, defaultValue, hasDefaultValue, validValues, context, converter)
}

// CreateConnectionPropertyWithExtension is used to create a new ConnectionProperty with a specific type and an
// extension. Properties with an extension can be created dynamically during the lifetime of a connection. These are
// lost when the connection is reset to its original state.
func CreateConnectionPropertyWithExtension[T comparable](extension, name, description string, defaultValue T, hasDefaultValue bool, validValues []T, context Context, converter func(value string) (T, error)) *TypedConnectionProperty[T] {
	var key string
	if extension == "" {
		key = name
	} else {
		key = extension + "." + name
	}
	return &TypedConnectionProperty[T]{
		key:             key,
		extension:       extension,
		name:            name,
		description:     description,
		defaultValue:    defaultValue,
		hasDefaultValue: hasDefaultValue,
		validValues:     validValues,
		context:         context,
		converter:       converter,
	}
}

var _ ConnectionProperty = (*TypedConnectionProperty[any])(nil)

// TypedConnectionProperty implements the ConnectionProperty interface.
// All fields are unexported to ensure that the values can only be updated in accordance with the semantics of the
// chosen ConnectionState Type.
type TypedConnectionProperty[T comparable] struct {
	key          string
	extension    string
	name         string
	description  string
	defaultValue T
	// hasDefaultValue indicates whether this property has a default value that is different from the default that
	// Spanner would otherwise use. Put another way: Should this default value be included in requests to Spanner,
	// or should the field in the request be kept unset in order to let Spanner choose the default.
	hasDefaultValue bool
	validValues     []T
	context         Context
	converter       func(string) (T, error)
}

func (p *TypedConnectionProperty[T]) String() string {
	return p.Key()
}

func (p *TypedConnectionProperty[T]) Key() string {
	return p.key
}

func (p *TypedConnectionProperty[T]) Context() Context {
	return p.context
}

// CreateDefaultValue implements ConnectionProperty.CreateDefaultValue.
func (p *TypedConnectionProperty[T]) CreateDefaultValue() ConnectionPropertyValue {
	return &connectionPropertyValue[T]{
		connectionProperty: p,
		resetValue:         p.defaultValue,
		hasResetValue:      p.hasDefaultValue,
		value:              p.defaultValue,
		hasValue:           p.hasDefaultValue,
		removeAtReset:      false,
	}
}

// CreateInitialValue implements ConnectionProperty.CreateInitialValue.
func (p *TypedConnectionProperty[T]) CreateInitialValue(value any) (ConnectionPropertyValue, error) {
	valueT, ok := value.(T)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid type for value: %T", value)
	}
	return p.CreateTypedInitialValue(valueT), nil
}

// CreateTypedInitialValue creates an initial value for a connection property with a known type.
func (p *TypedConnectionProperty[T]) CreateTypedInitialValue(value T) ConnectionPropertyValue {
	return &connectionPropertyValue[T]{
		connectionProperty: p,
		resetValue:         value,
		hasResetValue:      true,
		value:              value,
		hasValue:           true,
		removeAtReset:      false,
	}
}

func (p *TypedConnectionProperty[T]) Convert(value string) (any, error) {
	return p.converter(value)
}

// GetValue implements ConnectionPropertyValue.GetValue
func (p *TypedConnectionProperty[T]) GetValue(state *ConnectionState) (any, bool, error) {
	return p.GetValueOrError(state)
}

// GetValueOrDefault returns the current value of the property in the given ConnectionState.
// It returns the default of the property if no value is found.
func (p *TypedConnectionProperty[T]) GetValueOrDefault(state *ConnectionState) T {
	value, _ := state.value(p /*returnErrForUnknownProperty=*/, false)
	if value == nil {
		return p.defaultValue
	}
	if typedValue, ok := value.(*connectionPropertyValue[T]); ok {
		return typedValue.value
	}
	return p.defaultValue
}

// GetConnectionPropertyValue returns a reference to the ConnectionPropertyValue in the given ConnectionState.
// The function returns nil if the property does not exist.
// The function returns the default value if the property exists, but there is no value for the property in the given
// ConnectionState.
func (p *TypedConnectionProperty[T]) GetConnectionPropertyValue(state *ConnectionState) ConnectionPropertyValue {
	value, _ := state.value(p /*returnErrForUnknownProperty=*/, false)
	if value == nil {
		return nil
	}
	if typedValue, ok := value.(*connectionPropertyValue[T]); ok {
		return typedValue
	}
	return p.CreateDefaultValue()
}

// GetValueOrError returns the current value of the property in the given ConnectionState.
// It returns an error if no value is found.
func (p *TypedConnectionProperty[T]) GetValueOrError(state *ConnectionState) (T, bool, error) {
	value, err := state.value(p /*returnErrForUnknownProperty=*/, true)
	if err != nil {
		return p.zeroAndErr(err)
	}
	if value == nil {
		return p.zeroAndErr(status.Errorf(codes.InvalidArgument, "no value found for property: %q", p))
	}
	if typedValue, ok := value.(*connectionPropertyValue[T]); ok {
		return typedValue.value, typedValue.hasValue, nil
	}
	return p.zeroAndErr(status.Errorf(codes.InvalidArgument, "value has wrong type: %s", value))
}

// ResetValue resets the value of the property in the given ConnectionState to its original value.
//
// The given Context should indicate the current context where the application tries to reset the value, e.g. it should
// be ContextUser if the reset happens during the lifetime of a connection, and ContextStartup if the reset happens at
// the creation of a connection.
func (p *TypedConnectionProperty[T]) ResetValue(state *ConnectionState, context Context) error {
	value, _ := state.value(p /*returnErrForUnknownProperty=*/, false)
	if value == nil {
		return p.setConnectionStateValue(state, p.defaultValue, setValueCommandReset, context)
	}
	resetValue := value.GetResetValue()
	typedResetValue, ok := resetValue.(T)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "value has wrong type: %T", resetValue)
	}
	return p.setConnectionStateValue(state, typedResetValue, setValueCommandReset, context)
}

// SetUntypedValue implements ConnectionProperty.SetUntypedValue.
func (p *TypedConnectionProperty[T]) SetUntypedValue(state *ConnectionState, value any, context Context) error {
	valueT, ok := value.(T)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "invalid type for value: %T", value)
	}
	return p.SetValue(state, valueT, context)
}

// SetDefaultValue implements ConnectionProperty.SetDefaultValue.
func (p *TypedConnectionProperty[T]) SetDefaultValue(state *ConnectionState, context Context) error {
	return p.setConnectionStateValue(state, p.defaultValue, setValueCommandDefault, context)
}

// SetValue sets the value of the property in the given ConnectionState.
//
// The given Context should indicate the current context where the application tries to reset the value, e.g. it should
// be ContextUser if the reset happens during the lifetime of a connection, and ContextStartup if the reset happens at
// the creation of a connection.
func (p *TypedConnectionProperty[T]) SetValue(state *ConnectionState, value T, context Context) error {
	return p.setConnectionStateValue(state, value, setValueCommandSet, context)
}

func (p *TypedConnectionProperty[T]) setConnectionStateValue(state *ConnectionState, value T, setValueCommand setValueCommand, context Context) error {
	if p.context < context {
		return status.Errorf(codes.FailedPrecondition, "property has context %s and cannot be set in context %s", p.context, context)
	}
	if !state.inTransaction || state.connectionStateType == TypeNonTransactional || context < ContextUser {
		// Set the value in non-transactional mode.
		if err := p.setValue(state, state.properties, value, setValueCommand, context); err != nil {
			return err
		}
		// Remove the setting from the local settings if it's there, as the new setting is
		// the one that should be used.
		if state.localProperties != nil {
			delete(state.localProperties, p.key)
		}
		return nil
	}
	// Set the value in a transaction.
	if state.transactionProperties == nil {
		state.transactionProperties = make(map[string]ConnectionPropertyValue)
	}
	if err := p.setValue(state, state.transactionProperties, value, setValueCommand, context); err != nil {
		return err
	}
	// Remove the setting from the local settings if it's there, as the new transaction setting is
	// the one that should be used.
	if state.localProperties != nil {
		delete(state.localProperties, p.key)
	}
	return nil
}

// SetLocalUntypedValue implements ConnectionProperty.SetLocalUntypedValue.
func (p *TypedConnectionProperty[T]) SetLocalUntypedValue(state *ConnectionState, value any) error {
	valueT, ok := value.(T)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "invalid type for value: %T", value)
	}
	return p.SetLocalValue(state, valueT)
}

// SetLocalDefaultValue implements ConnectionProperty.SetLocalDefaultValue.
func (p *TypedConnectionProperty[T]) SetLocalDefaultValue(state *ConnectionState) error {
	return p.setLocalValue(state, p.defaultValue, setValueCommandDefault)
}

// ResetLocalValue resets the local value of the property in the given ConnectionState to its original value.
func (p *TypedConnectionProperty[T]) ResetLocalValue(state *ConnectionState) error {
	value, err := state.value(p /*returnErrForUnknownProperty=*/, true)
	if err != nil {
		return err
	}
	resetValue := value.GetResetValue()
	typedResetValue, ok := resetValue.(T)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "value has wrong type: %T", resetValue)
	}
	return p.setLocalValue(state, typedResetValue, setValueCommandReset)
}

// SetLocalValue sets the local value of the property in the given ConnectionState. A local value is only visible
// for the remainder of the current transaction. The value is reset to the value it had before the transaction when the
// transaction ends, regardless whether the transaction committed or rolled back.
//
// Setting a local value outside a transaction is a no-op.
func (p *TypedConnectionProperty[T]) SetLocalValue(state *ConnectionState, value T) error {
	return p.setLocalValue(state, value, setValueCommandSet)
}

func (p *TypedConnectionProperty[T]) setLocalValue(state *ConnectionState, value T, setValueCommand setValueCommand) error {
	if p.context < ContextUser {
		return status.Error(codes.FailedPrecondition, "SetLocalValue is only supported for properties with context USER or higher")
	}
	if !state.inTransaction {
		// SetLocalValue outside a transaction is a no-op.
		return nil
	}
	if state.localProperties == nil {
		state.localProperties = make(map[string]ConnectionPropertyValue)
	}
	return p.setValue(state, state.localProperties, value, setValueCommand, ContextUser)
}

func (p *TypedConnectionProperty[T]) setValue(state *ConnectionState, currentProperties map[string]ConnectionPropertyValue, value T, setValueCommand setValueCommand, context Context) error {
	if err := p.checkValidValue(value); err != nil {
		return err
	}
	newValue, ok := currentProperties[p.key]
	if !ok {
		existingValue, ok := state.properties[p.key]
		if !ok {
			if p.extension == "" || setValueCommand == setValueCommandReset {
				return unknownPropertyErr(p)
			}
			newValue = &connectionPropertyValue[T]{connectionProperty: p, removeAtReset: true}
		} else {
			newValue = existingValue.Copy()
		}
	}
	if err := newValue.SetValue(value, setValueCommand, context); err != nil {
		return err
	}
	currentProperties[p.key] = newValue
	return nil
}

func (p *TypedConnectionProperty[T]) zeroAndErr(err error) (T, bool, error) {
	var t T
	return t, false, err
}

func (p *TypedConnectionProperty[T]) checkValidValue(value T) error {
	if p.validValues == nil {
		return nil
	}
	for _, validValue := range p.validValues {
		if value == validValue {
			return nil
		}
	}
	return nil
}

func unknownPropertyErr(p ConnectionProperty) error {
	return status.Errorf(codes.InvalidArgument, "unrecognized configuration property %q", p.Key())
}

// ConnectionPropertyValue is the public interface for connection state property values.
type ConnectionPropertyValue interface {
	// ConnectionProperty returns the property that this value is for.
	ConnectionProperty() ConnectionProperty
	// Copy creates a shallow copy of the ConnectionPropertyValue.
	Copy() ConnectionPropertyValue
	// HasValue indicates whether this ConnectionPropertyValue has an explicit value.
	HasValue() bool
	// GetValue gets the current value of the property.
	GetValue() (any, error)
	// ClearValue removes the value of the property.
	ClearValue(context Context) error
	// SetValue sets the value of the property. The given value must be a valid value for the property.
	SetValue(value any, setValueCommand setValueCommand, context Context) error
	// ResetValue resets the value of the property to the value it had at the creation of the connection.
	// This method should only be called when resetting the entire connection state.
	ResetValue(context Context) error
	// RemoveAtReset indicates whether the value should be removed from the ConnectionState when the ConnectionState is
	// reset. This function should return true for property values that have been added to the set after the connection
	// was created, for example because the user executed `set my_extension.my_property='some-value'`.
	RemoveAtReset() bool
	// GetResetValue returns the value that will be assigned to this property value if the value is reset.
	GetResetValue() any

	isRemoved() bool
}

type connectionPropertyValue[T comparable] struct {
	connectionProperty *TypedConnectionProperty[T]
	resetValue         T
	hasResetValue      bool
	value              T
	hasValue           bool
	removeAtReset      bool
	removed            bool
}

func (v *connectionPropertyValue[T]) ConnectionProperty() ConnectionProperty {
	return v.connectionProperty
}

func (v *connectionPropertyValue[T]) Copy() ConnectionPropertyValue {
	return &connectionPropertyValue[T]{
		connectionProperty: v.connectionProperty,
		resetValue:         v.resetValue,
		hasResetValue:      v.hasResetValue,
		value:              v.value,
		hasValue:           v.hasValue,
		removeAtReset:      v.removeAtReset,
	}
}

func (v *connectionPropertyValue[T]) HasValue() bool {
	return v.hasValue
}

func (v *connectionPropertyValue[T]) GetValue() (any, error) {
	return v.value, nil
}

func (v *connectionPropertyValue[T]) ClearValue(context Context) error {
	if v.connectionProperty.context < context {
		return status.Errorf(codes.FailedPrecondition, "property has context %s and cannot be set in context %s", v.connectionProperty.context, context)
	}
	v.value = v.connectionProperty.defaultValue
	v.hasValue = false
	return nil
}

type setValueCommand int

const (
	setValueCommandSet setValueCommand = iota
	setValueCommandReset
	setValueCommandDefault
)

func (v *connectionPropertyValue[T]) SetValue(value any, setValueCommand setValueCommand, context Context) error {
	if v.connectionProperty.context < context {
		return status.Errorf(codes.FailedPrecondition, "property has context %s and cannot be set in context %s", v.connectionProperty.context, context)
	}
	typedValue, ok := value.(T)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "value has wrong type: %T", value)
	}
	v.value = typedValue
	switch setValueCommand {
	case setValueCommandSet:
		v.hasValue = true
	case setValueCommandReset:
		v.hasValue = v.hasResetValue
		v.removed = v.removeAtReset
	case setValueCommandDefault:
		v.hasValue = v.connectionProperty.hasDefaultValue
	}
	return nil
}

func (v *connectionPropertyValue[T]) ResetValue(context Context) error {
	if v.connectionProperty.context < context {
		return status.Errorf(codes.FailedPrecondition, "property has context %s and cannot be set in context %s", v.connectionProperty.context, context)
	}
	v.value = v.resetValue
	v.hasValue = v.hasResetValue
	return nil
}

func (v *connectionPropertyValue[T]) RemoveAtReset() bool {
	return v.removeAtReset
}

func (v *connectionPropertyValue[T]) GetResetValue() any {
	return v.resetValue
}

func (v *connectionPropertyValue[T]) isRemoved() bool {
	return v.removed
}
