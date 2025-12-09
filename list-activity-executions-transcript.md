# ListActivityExecutions and CountActivityExecutions Implementation Guide

This document is a cleaned-up transcript from a call with Roey explaining how to implement `ListActivityExecutions` and `CountActivityExecutions` APIs for standalone activities using the CHASM framework.

---

## Overview

To implement listing and counting for standalone activities, you need to:
1. Integrate with the visibility provider interfaces
2. Define search attributes for the activity archetype
3. Implement the `VisibilitySearchAttributesProvider` and `VisibilityMemoProvider` interfaces
4. Create a proto message for the activity list info (memo)
5. Handle search attribute validation in the frontend without persisting unaliased versions
6. Inject the CHASM Visibility Manager into the frontend handler

---

## Visibility Provider Interfaces

The first thing you need to do is integrate with the provider interfaces. There are two:

https://github.com/temporalio/temporal/blob/standalone-activity/chasm/visibility.go

`chasm/visibility.go` (`VisibilitySearchAttributesProvider`)
```go
// VisibilitySearchAttributesProvider if implemented by the root Component,
// allows the CHASM framework to automatically determine, at the end of
// a transaction, if a visibility task needs to be generated to update the
// visibility record with the returned search attributes.
type VisibilitySearchAttributesProvider interface {
	SearchAttributes(Context) []SearchAttributeKeyValue
}
```

https://github.com/temporalio/temporal/blob/standalone-activity/chasm/visibility.go

`chasm/visibility.go` (`VisibilityMemoProvider`)
```go
// VisibilityMemoProvider if implemented by the root Component,
// allows the CHASM framework to automatically determine, at the end of
// a transaction, if a visibility task needs to be generated to update the
// visibility record with the returned memo.
type VisibilityMemoProvider interface {
	Memo(Context) proto.Message
}
```

---

## CHASM Search Attributes

When you register your component, you register it with search attributes. Based on the spec (blueprint), you'll want:
- Activity type as a search attribute
- Task queue
- Execution status (using the low cardinality keyword field)

### Available CHASM Search Attribute Fields

CHASM provides predefined fields that map to persistence columns. Each archetype can use each field only once:

https://github.com/temporalio/temporal/blob/standalone-activity/chasm/search_attribute.go

`chasm/search_attribute.go` (search attribute field definitions)
```go
var (
	SearchAttributeFieldBool01 = newSearchAttributeFieldBool(1)
	SearchAttributeFieldBool02 = newSearchAttributeFieldBool(2)

	SearchAttributeFieldDateTime01 = newSearchAttributeFieldDateTime(1)
	SearchAttributeFieldDateTime02 = newSearchAttributeFieldDateTime(2)

	SearchAttributeFieldInt01 = newSearchAttributeFieldInt(1)
	SearchAttributeFieldInt02 = newSearchAttributeFieldInt(2)

	SearchAttributeFieldDouble01 = newSearchAttributeFieldDouble(1)
	SearchAttributeFieldDouble02 = newSearchAttributeFieldDouble(2)

	SearchAttributeFieldKeyword01 = newSearchAttributeFieldKeyword(1)
	SearchAttributeFieldKeyword02 = newSearchAttributeFieldKeyword(2)
	SearchAttributeFieldKeyword03 = newSearchAttributeFieldKeyword(3)
	SearchAttributeFieldKeyword04 = newSearchAttributeFieldKeyword(4)

	SearchAttributeFieldKeywordList01 = newSearchAttributeFieldKeywordList(1)
	SearchAttributeFieldKeywordList02 = newSearchAttributeFieldKeywordList(2)
	// ...
)
```

### Predefined System Search Attributes

These are existing search attributes that can be used from CHASM, but you probably won't need most of them. The relevant ones might be `BuildIds` for versioning integration:

https://github.com/temporalio/temporal/blob/standalone-activity/chasm/search_attribute.go

`chasm/search_attribute.go` (predefined search attributes)
```go
	SearchAttributeTemporalChangeVersion              = newSearchAttributeKeywordListByField(sadefs.TemporalChangeVersion)
	SearchAttributeBinaryChecksums                    = newSearchAttributeKeywordListByField(sadefs.BinaryChecksums)
	SearchAttributeBuildIds                           = newSearchAttributeKeywordListByField(sadefs.BuildIds)
	// ...
	SearchAttributeTemporalNamespaceDivision          = newSearchAttributeKeywordByField(sadefs.TemporalNamespaceDivision)
	SearchAttributeTemporalPauseInfo                  = newSearchAttributeKeywordListByField(sadefs.TemporalPauseInfo)
	SearchAttributeTemporalReportedProblems           = newSearchAttributeKeywordListByField(sadefs.TemporalReportedProblems)
```

### Low Cardinality Keyword Field

**Important:** There is only one low cardinality keyword field available per archetype. This is what you want to use for `ExecutionStatus`. The low cardinality keyword is the only field that supports `COUNT GROUP BY` queries.

The UI uses this for showing execution status counts at the top of the list view (e.g., "10 Running, 5 Failed, 3 Completed").

> **[MARKER: Need to verify where the low cardinality keyword field is defined in CHASM - it may not be implemented yet or may be one of the existing keyword fields]**

---

## Defining Search Attributes for Your Component

Here's an example of how search attributes are defined for the test PayloadStore component:

https://github.com/temporalio/temporal/blob/standalone-activity/chasm/lib/tests/payload.go

`chasm/lib/tests/payload.go` (search attribute definitions)
```go
var (
	PayloadTotalCountSearchAttribute = chasm.NewSearchAttributeInt(PayloadTotalCountSAAlias, chasm.SearchAttributeFieldInt01)
	PayloadTotalSizeSearchAttribute  = chasm.NewSearchAttributeInt(PayloadTotalSizeSAAlias, chasm.SearchAttributeFieldInt02)

	_ chasm.VisibilitySearchAttributesProvider = (*PayloadStore)(nil)
	_ chasm.VisibilityMemoProvider             = (*PayloadStore)(nil)
)
```

### Registering Search Attributes with the Library

When registering your component, use the `WithSearchAttributes` option:

https://github.com/temporalio/temporal/blob/standalone-activity/chasm/lib/tests/library.go

`chasm/lib/tests/library.go` (`library.Components`)
```go
func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*PayloadStore]("payloadStore",
			chasm.WithSearchAttributes(
				PayloadTotalCountSearchAttribute,
				PayloadTotalSizeSearchAttribute,
				chasm.SearchAttributeTemporalScheduledByID,
			),
		),
	}
}
```

---

## How CHASM Visibility Works

CHASM as a framework handles visibility automatically:

1. Every time you mutate your root component, if you have a visibility component attached, CHASM will:
   - Call the `SearchAttributes()` function at the beginning of a transaction
   - Record the current set of search attributes
   - Run the mutation
   - Call `SearchAttributes()` again after the mutation
   - Compare if anything changed in the search attributes or memo
   - Generate a visibility task to update visibility if something changed

This is handled automatically - you don't have to do the comparisons yourself. You just implement the interface to return the current state.

---

## Creating the Visibility Component

https://github.com/temporalio/temporal/blob/standalone-activity/chasm/visibility.go

`chasm/visibility.go` (`NewVisibilityWithData`)
```go
func NewVisibilityWithData(
	mutableContext MutableContext,
	customSearchAttributes map[string]*commonpb.Payload,
	customMemo map[string]*commonpb.Payload,
) *Visibility {
	visibility := &Visibility{
		Data: &persistencespb.ChasmVisibilityData{
			TransitionCount: 0,
		},
		SA: NewDataField(
			mutableContext,
			&commonpb.SearchAttributes{IndexedFields: customSearchAttributes},
		),
		Memo: NewDataField(
			mutableContext,
			&commonpb.Memo{Fields: customMemo},
		),
	}

	visibility.generateTask(mutableContext)
	return visibility
}
```

When you construct the visibility component, pass in all of the user-provided search attributes from the start request. There's no user-defined memo for standalone activities (we decided we don't need it), so just pass the search attributes.

---

## Implementing the Provider Interfaces

### SearchAttributes Implementation

https://github.com/temporalio/temporal/blob/standalone-activity/chasm/lib/tests/payload.go

`chasm/lib/tests/payload.go` (`PayloadStore.SearchAttributes`)
```go
// SearchAttributes implements chasm.VisibilitySearchAttributesProvider interface
func (s *PayloadStore) SearchAttributes(
	ctx chasm.Context,
) []chasm.SearchAttributeKeyValue {
	return []chasm.SearchAttributeKeyValue{
		PayloadTotalCountSearchAttribute.Value(s.State.TotalCount),
		PayloadTotalSizeSearchAttribute.Value(s.State.TotalSize),
		chasm.SearchAttributeTemporalScheduledByID.Value(TestScheduleID),
	}
}
```

This returns typed key-value pairs. The type safety is enforced at compile time - you can't get this wrong because `Value()` expects the correct type for each search attribute.

### Memo Implementation

https://github.com/temporalio/temporal/blob/standalone-activity/chasm/lib/tests/payload.go

`chasm/lib/tests/payload.go` (`PayloadStore.Memo`)
```go
// Memo implements chasm.VisibilityMemoProvider interface
func (s *PayloadStore) Memo(_ chasm.Context) proto.Message {
	return s.State
}
```

The memo must return a proto.Message. This is important because the memo is used to fulfill the list response without having to read the full execution record from history.

### Why We Need a Memo

When you list executions, you don't want to read each execution record from history to construct the list result. The visibility index already has enough information to answer the query. The memo is the extra data attached to each visibility record that gets returned from the visibility store, allowing you to fulfill the `ListActivityExecutions` response directly.

For workflows, this is already in the visibility schema as a blob field:

https://github.com/temporalio/temporal/blob/standalone-activity/schema/mysql/v8/visibility/schema.sql

`schema/mysql/v8/visibility/schema.sql` (memo column)
```sql
CREATE TABLE executions_visibility (
  -- ...
  memo                    BLOB          NULL,
  -- ...
);
```

This blob contains a structured protobuf. For workflows, there's a dedicated field. For CHASM archetypes, there's also CHASM-specific memo data in addition to any user-defined key-value memo.

---

## Search Attribute Validation in the Frontend

### The Aliasing Problem

In the frontend, there's validation logic that handles search attribute aliasing. The key function is:

https://github.com/temporalio/temporal/blob/standalone-activity/service/frontend/workflow_handler.go

`service/frontend/workflow_handler.go` (`WorkflowHandler.unaliasedSearchAttributesFrom`)
```go
func (wh *WorkflowHandler) unaliasedSearchAttributesFrom(
	attributes *commonpb.SearchAttributes,
	namespaceName namespace.Name,
) (*commonpb.SearchAttributes, error) {
	sa, err := searchattribute.UnaliasFields(wh.saMapperProvider, attributes, namespaceName.String())
	if err != nil {
		return nil, err
	}

	if err = wh.validateSearchAttributes(sa, namespaceName); err != nil {
		return nil, err
	}
	return sa, nil
}
```

This function:
1. Takes user-provided search attributes (aliased, user-friendly names)
2. Maps them to the actual database field names (unaliased)
3. Validates the types and sizes

### Critical: Don't Persist Unaliased Search Attributes

**For CHASM archetypes, you must NOT persist the unaliased (mapped) search attributes.**

What workflows do (which we don't want to replicate):
- Unalias the search attributes
- Validate them
- Store the unaliased version in history/persistence

What CHASM archetypes should do:
- Unalias the search attributes for validation only
- Validate the types and sizes
- **Store the original aliased version** (the user-friendly names)
- Let CHASM handle the aliasing/unaliasing when reading from visibility

This is important because:
1. When you inspect the data in persistence, you see the user-friendly names
2. Any error messages use the user-friendly names
3. The mapped database field names never leak to the user

The code should do validation but NOT override the indexed fields on the search attributes.

---

## Search Attribute Collision Handling

All CHASM search attributes are automatically prefixed with `Temporal` in storage. This is important because:

1. Users can already have custom search attributes defined (e.g., a user might have defined `ActivityType`)
2. Nobody guarantees that our new CHASM search attributes won't collide with user-defined ones
3. Users can't define search attributes with the `Temporal` prefix (reserved)

When there's a collision:
- User search attributes take precedence
- Users can specify `TemporalActivityType` to target the CHASM-defined system search attribute
- The UI will want to use fully qualified names (e.g., `TemporalActivityType`) for built-in queries

---

## Using the List and Count APIs

### ListExecutions

https://github.com/temporalio/temporal/blob/standalone-activity/chasm/visibility_manager.go

`chasm/visibility_manager.go` (`ListExecutions`)
```go
func ListExecutions[C Component, M proto.Message](
	ctx context.Context,
	request *ListExecutionsRequest,
) (*ListExecutionsResponse[M], error) {
	archetypeType := reflect.TypeFor[C]()
	response, err := visibilityManagerFromContext(ctx).ListExecutions(ctx, archetypeType, request)
	if err != nil {
		return nil, err
	}

	// Convert response, unmarshaling ChasmMemo to type M
	executions := make([]*ExecutionInfo[M], len(response.Executions))
	for i, execution := range response.Executions {
		// ... unmarshaling logic ...
	}

	return &ListExecutionsResponse[M]{
		Executions:    executions,
		NextPageToken: response.NextPageToken,
	}, nil
}
```

You specify:
- `C`: The component type (for filtering and search attribute registration)
- `M`: The memo proto message type

The memo type you specify here is what you return from the `Memo()` method on your component.

### ExecutionInfo Structure

https://github.com/temporalio/temporal/blob/standalone-activity/chasm/visibility_manager.go

`chasm/visibility_manager.go` (`ExecutionInfo`)
```go
type ExecutionInfo[M proto.Message] struct {
	BusinessID             string
	RunID                  string
	StartTime              time.Time
	CloseTime              time.Time
	HistoryLength          int64
	HistorySizeBytes       int64
	StateTransitionCount   int64
	ChasmSearchAttributes  SearchAttributesMap
	CustomSearchAttributes map[string]*commonpb.Payload
	Memo                   *commonpb.Memo
	ChasmMemo              M
}
```

What you get back includes:
- Pre-populated fields (BusinessID, RunID, StartTime, CloseTime, etc.)
- `ChasmSearchAttributes`: Your archetype's search attributes
- `CustomSearchAttributes`: User-defined custom search attributes
- `ChasmMemo`: Your proto message (automatically deserialized)

If you emit something as a search attribute, you don't also have to store it in the memo - search attributes are available separately.

### CountExecutions

https://github.com/temporalio/temporal/blob/standalone-activity/chasm/visibility_manager.go

`chasm/visibility_manager.go` (`CountExecutions`)
```go
func CountExecutions[C Component](
	ctx context.Context,
	request *CountExecutionsRequest,
) (*CountExecutionsResponse, error) {
	archetypeType := reflect.TypeFor[C]()
	return visibilityManagerFromContext(ctx).CountExecutions(ctx, archetypeType, request)
}
```

For count with `GROUP BY`, the response will have:
- `Count`: Total count
- `Groups`: Array of group values with counts

> **[MARKER: The current CHASM `CountExecutionsResponse` only has `Count` field - the `Groups` field for GROUP BY support may need to be added]**

---

## Frontend Handler Integration

The List and Count APIs are served directly from the frontend - they don't go to the history service.

### Injecting the CHASM Visibility Manager

You need to inject the `ChasmVisibilityManager` into the frontend. The test setup shows how:

https://github.com/temporalio/temporal/blob/standalone-activity/tests/chasm_test.go

`tests/chasm_test.go` (`ChasmTestSuite.SetupSuite`)
```go
func (s *ChasmTestSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key():                           true,
			dynamicconfig.VisibilityEnableUnifiedQueryConverter.Key(): s.enableUnifiedQueryConverter,
		}),
	)

	chasmEngine, err := s.FunctionalTestBase.GetTestCluster().Host().ChasmEngine()
	s.Require().NoError(err)
	s.Require().NotNil(chasmEngine)

	chasmVisibilityMgr := s.GetTestCluster().Host().ChasmVisibilityManager()
	s.Require().NotNil(chasmVisibilityMgr)

	s.chasmContext = chasm.NewEngineContext(context.Background(), chasmEngine)
	s.chasmContext = chasm.NewVisibilityManagerContext(s.chasmContext, chasmVisibilityMgr)
}
```

In your frontend FX module, you'll need to ensure the visibility manager is injected as a dependency.

---

## Writing Functional Tests

### Using Eventually for Visibility APIs

Visibility APIs are eventually consistent, so use `Eventually` with a timeout:

https://github.com/temporalio/temporal/blob/standalone-activity/tests/chasm_test.go

`tests/chasm_test.go` (`ChasmTestSuite.TestPayloadStoreVisibility`)
```go
var visRecord *chasm.ExecutionInfo[*testspb.TestPayloadStore]
s.Eventually(
	func() bool {
		resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
			NamespaceID:   string(s.NamespaceID()),
			NamespaceName: string(s.Namespace()),
			PageSize:      10,
			Query:         visQuery,
		})
		s.NoError(err)
		if len(resp.Executions) != 1 {
			return false
		}

		visRecord = resp.Executions[0]
		return true
	},
	testcore.WaitForESToSettle,
	100*time.Millisecond,
)
```

### Test Coverage

Your tests should:
1. Start an activity execution with custom search attributes
2. Query based on the activity fields (activity type, task queue, status)
3. Query based on custom search attributes
4. Verify that list info (memo fields) are populated correctly
5. Verify custom search attributes are attached properly

### Custom Search Attributes in Functional Tests

Custom search attributes are shared across archetypes - they're namespace-level, not archetype-level. So if you define `CustomerID`, it can be used for standalone activities and workflows.

The functional test framework should already have search attributes registered when the namespace is created.

---

## Summary: Implementation Steps

1. **Define search attributes** - Based on the blueprint, define search attributes for activity type, task queue, execution status, etc.

2. **Create a proto message for the memo** - This will be the `ActivityListInfo` or similar that contains the data needed to fulfill list responses without reading from history.

3. **Implement the interfaces** - Implement `VisibilitySearchAttributesProvider` and `VisibilityMemoProvider` on your activity root component.

4. **Register with search attributes** - Use `WithSearchAttributes()` when registering the component.

5. **Handle validation in frontend** - Do `UnaliasFields` for validation, but don't persist the unaliased version.

6. **Implement list API** - Inject `ChasmVisibilityManager`, call `ListExecutions`, map the response to the protobuf format.

7. **Implement count API** - Similar pattern, using `CountExecutions`.

8. **Write functional tests** - Test querying by various search attributes, verify memo fields, use `Eventually` for eventually consistent checks.

---

## Reference Implementation

Use the PayloadStore test component as your primary reference:

- `chasm/lib/tests/payload.go` - Search attributes, provider implementations
- `chasm/lib/tests/library.go` - Component registration
- `tests/chasm_test.go` - Functional tests for visibility

**Don't use the scheduler implementation as a reference** - it uses the older workflow-based approach that doesn't use the CHASM visibility framework.

