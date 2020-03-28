// The MIT License (MIT)
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

// Code generated by MockGen. DO NOT EDIT.
// Source: clientInterface.go

// Package dynamicconfig is a generated GoMock package.
package dynamicconfig

import (
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
	time "time"
)

// MockClient is a mock of Client interface.
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *MockClientMockRecorder
}

// MockClientMockRecorder is the mock recorder for MockClient.
type MockClientMockRecorder struct {
	mock *MockClient
}

// NewMockClient creates a new mock instance.
func NewMockClient(ctrl *gomock.Controller) *MockClient {
	mock := &MockClient{ctrl: ctrl}
	mock.recorder = &MockClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClient) EXPECT() *MockClientMockRecorder {
	return m.recorder
}

// GetValue mocks base method.
func (m *MockClient) GetValue(name Key, defaultValue interface{}) (interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetValue", name, defaultValue)
	ret0, _ := ret[0].(interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetValue indicates an expected call of GetValue.
func (mr *MockClientMockRecorder) GetValue(name, defaultValue interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetValue", reflect.TypeOf((*MockClient)(nil).GetValue), name, defaultValue)
}

// GetValueWithFilters mocks base method.
func (m *MockClient) GetValueWithFilters(name Key, filters map[Filter]interface{}, defaultValue interface{}) (interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetValueWithFilters", name, filters, defaultValue)
	ret0, _ := ret[0].(interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetValueWithFilters indicates an expected call of GetValueWithFilters.
func (mr *MockClientMockRecorder) GetValueWithFilters(name, filters, defaultValue interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetValueWithFilters", reflect.TypeOf((*MockClient)(nil).GetValueWithFilters), name, filters, defaultValue)
}

// GetIntValue mocks base method.
func (m *MockClient) GetIntValue(name Key, filters map[Filter]interface{}, defaultValue int) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetIntValue", name, filters, defaultValue)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetIntValue indicates an expected call of GetIntValue.
func (mr *MockClientMockRecorder) GetIntValue(name, filters, defaultValue interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetIntValue", reflect.TypeOf((*MockClient)(nil).GetIntValue), name, filters, defaultValue)
}

// GetFloatValue mocks base method.
func (m *MockClient) GetFloatValue(name Key, filters map[Filter]interface{}, defaultValue float64) (float64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetFloatValue", name, filters, defaultValue)
	ret0, _ := ret[0].(float64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetFloatValue indicates an expected call of GetFloatValue.
func (mr *MockClientMockRecorder) GetFloatValue(name, filters, defaultValue interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetFloatValue", reflect.TypeOf((*MockClient)(nil).GetFloatValue), name, filters, defaultValue)
}

// GetBoolValue mocks base method.
func (m *MockClient) GetBoolValue(name Key, filters map[Filter]interface{}, defaultValue bool) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBoolValue", name, filters, defaultValue)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetBoolValue indicates an expected call of GetBoolValue.
func (mr *MockClientMockRecorder) GetBoolValue(name, filters, defaultValue interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBoolValue", reflect.TypeOf((*MockClient)(nil).GetBoolValue), name, filters, defaultValue)
}

// GetStringValue mocks base method.
func (m *MockClient) GetStringValue(name Key, filters map[Filter]interface{}, defaultValue string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetStringValue", name, filters, defaultValue)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetStringValue indicates an expected call of GetStringValue.
func (mr *MockClientMockRecorder) GetStringValue(name, filters, defaultValue interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStringValue", reflect.TypeOf((*MockClient)(nil).GetStringValue), name, filters, defaultValue)
}

// GetMapValue mocks base method.
func (m *MockClient) GetMapValue(name Key, filters map[Filter]interface{}, defaultValue map[string]interface{}) (map[string]interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMapValue", name, filters, defaultValue)
	ret0, _ := ret[0].(map[string]interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMapValue indicates an expected call of GetMapValue.
func (mr *MockClientMockRecorder) GetMapValue(name, filters, defaultValue interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMapValue", reflect.TypeOf((*MockClient)(nil).GetMapValue), name, filters, defaultValue)
}

// GetDurationValue mocks base method.
func (m *MockClient) GetDurationValue(name Key, filters map[Filter]interface{}, defaultValue time.Duration) (time.Duration, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetDurationValue", name, filters, defaultValue)
	ret0, _ := ret[0].(time.Duration)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetDurationValue indicates an expected call of GetDurationValue.
func (mr *MockClientMockRecorder) GetDurationValue(name, filters, defaultValue interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDurationValue", reflect.TypeOf((*MockClient)(nil).GetDurationValue), name, filters, defaultValue)
}

// UpdateValue mocks base method.
func (m *MockClient) UpdateValue(name Key, value interface{}) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateValue", name, value)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateValue indicates an expected call of UpdateValue.
func (mr *MockClientMockRecorder) UpdateValue(name, value interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateValue", reflect.TypeOf((*MockClient)(nil).UpdateValue), name, value)
}
