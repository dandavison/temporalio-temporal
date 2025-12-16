# Temporal Visibility Query Guide

This guide explains how users interact with Temporal's visibility system through the gRPC API, covering search attributes, queries, memos, and the differences between workflow and CHASM implementations.

## Overview

**Visibility** is Temporal's system for indexing and querying execution metadata. It enables users to list, filter, and count workflow or activity executions without reading their full history.

## Core Concepts

### Search Attributes

Search attributes are **indexed fields** that can be used in query filters. There are three categories:

#### 1. System Search Attributes

Built-in attributes automatically populated by Temporal. These cannot be modified by users.

[common/searchattribute/sadefs/constants.go](https://github.com/temporalio/temporal/blob/main/common/searchattribute/sadefs/constants.go) (`system`)

```go
system = map[string]enumspb.IndexedValueType{
    WorkflowID:           enumspb.INDEXED_VALUE_TYPE_KEYWORD,
    RunID:                enumspb.INDEXED_VALUE_TYPE_KEYWORD,
    WorkflowType:         enumspb.INDEXED_VALUE_TYPE_KEYWORD,
    StartTime:            enumspb.INDEXED_VALUE_TYPE_DATETIME,
    ExecutionTime:        enumspb.INDEXED_VALUE_TYPE_DATETIME,
    CloseTime:            enumspb.INDEXED_VALUE_TYPE_DATETIME,
    ExecutionStatus:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
    TaskQueue:            enumspb.INDEXED_VALUE_TYPE_KEYWORD,
    HistoryLength:        enumspb.INDEXED_VALUE_TYPE_INT,
    ExecutionDuration:    enumspb.INDEXED_VALUE_TYPE_INT,
    StateTransitionCount: enumspb.INDEXED_VALUE_TYPE_INT,
    HistorySizeBytes:     enumspb.INDEXED_VALUE_TYPE_INT,
    // ... parent/root workflow fields
}
```

#### 2. Custom Search Attributes

User-defined attributes set by SDKs during workflow/activity execution. These must be registered before use.

#### 3. Predefined/Reserved Search Attributes

Internal attributes used by Temporal features (schedules, versioning, etc.). Names starting with `Temporal` are reserved.

### Aliases

An **alias** is a user-friendly name that maps to an underlying storage field. This is particularly important for SQL-based visibility stores where custom search attributes use pre-allocated columns.

For example, a user might register a custom search attribute called `CustomerID`. In SQL stores, this maps to a pre-allocated field like `Keyword01`. The alias system allows users to query using `CustomerID` while Temporal handles the translation.

[common/persistence/visibility/store/query/resolve.go](https://github.com/temporalio/temporal/blob/main/common/persistence/visibility/store/query/resolve.go) (`ResolveSearchAttributeAlias`)

```go
func ResolveSearchAttributeAlias(
    name string,
    ns namespace.Name,
    mapper searchattribute.Mapper,
    saTypeMap searchattribute.NameTypeMap,
    chasmMapper *chasm.VisibilitySearchAttributesMapper,
) (string, enumspb.IndexedValueType, error) {
    if sadefs.IsMappable(name) {
        // First check if the visibility mapper can handle this field
        fieldName, fieldType := tryVisibilityMapper(name, ns, mapper, saTypeMap)
        if fieldName != "" {
            return fieldName, fieldType, nil
        }
        // ... try CHASM mapper for CHASM queries
    }
    // ... direct and prefixed lookup
}
```

### Memo

**Memo** is non-indexed data stored alongside visibility records. Unlike search attributes:

- Memos **cannot** be used in query filters
- Memos **are** returned in list responses
- Memos are useful for displaying additional execution details without reading the full execution

For workflows, memo is a user-provided `map[string]*Payload`. For CHASM archetypes, the memo can be a structured proto message defined by the component.

## Query Language

Visibility queries use a **SQL-like WHERE clause syntax**. The query is parsed using a SQL parser and converted to the underlying store's query format.

[common/persistence/visibility/store/query/converter.go](https://github.com/temporalio/temporal/blob/main/common/persistence/visibility/store/query/converter.go) (`QueryConverter.convertWhereString`)

```go
func (c *QueryConverter[ExprT]) convertWhereString(queryString string) (*QueryParams[ExprT], error) {
    where := strings.TrimSpace(queryString)
    if where != "" &&
        !strings.HasPrefix(strings.ToLower(where), "order by") &&
        !strings.HasPrefix(strings.ToLower(where), "group by") {
        where = "where " + where
    }
    // sqlparser can't parse just WHERE clause but instead accepts only valid SQL statement.
    sql := "select * from table1 " + where
    stmt, err := sqlparser.Parse(sql)
    // ...
}
```

### Supported Operations

| Feature | Support |
|---------|---------|
| Comparison operators | `=`, `!=`, `>`, `<`, `>=`, `<=` |
| Logical operators | `AND`, `OR`, `NOT` |
| `IN` clause | ✅ |
| `BETWEEN` | ✅ |
| `ORDER BY` | Elasticsearch only (deprecated) |
| `GROUP BY` | Single field only (for counts) |
| `LIMIT` | ❌ (use pagination) |
| String pattern matching | `LIKE` (limited) |

### Example Queries

```sql
-- Filter by workflow ID
WorkflowId = 'order-12345'

-- Filter by status and time range
ExecutionStatus = 'Running' AND StartTime > '2024-01-01T00:00:00Z'

-- Filter by custom search attribute
CustomerID = 'cust-789' AND OrderTotal > 100

-- Combined conditions
WorkflowType = 'OrderWorkflow' AND (ExecutionStatus = 'Completed' OR ExecutionStatus = 'Failed')
```

## Registering Custom Search Attributes

Before using custom search attributes in queries, they must be registered.

### Elasticsearch

Custom attributes are added directly to the cluster and stored in cluster metadata.

[service/frontend/operator_handler.go](https://github.com/temporalio/temporal/blob/main/service/frontend/operator_handler.go) (`OperatorHandlerImpl.addSearchAttributesElasticsearch`)

```go
func (h *OperatorHandlerImpl) addSearchAttributesElasticsearch(
    ctx context.Context,
    request *operatorservice.AddSearchAttributesRequest,
    visManager manager.VisibilityManager,
) error {
    // ... validation
    if err := visManager.AddSearchAttributes(
        ctx,
        &manager.AddSearchAttributesRequest{SearchAttributes: customAttributesToAdd},
    ); err != nil {
        return serviceerror.NewUnavailablef(errUnableToSaveSearchAttributesMessage, err)
    }
    // Save to cluster metadata
    err = h.saManager.SaveSearchAttributes(ctx, indexName, newCustomSearchAttributes)
    // ...
}
```

### SQL Databases

SQL stores use pre-allocated columns. Registration creates an alias mapping from the user's name to an available column.

[service/frontend/operator_handler.go](https://github.com/temporalio/temporal/blob/main/service/frontend/operator_handler.go) (`OperatorHandlerImpl.addSearchAttributesSQL`)

```go
func (h *OperatorHandlerImpl) addSearchAttributesSQL(/* ... */) error {
    // ... find first available field for the given type
    for fieldName, fieldType := range cmCustomSearchAttributes {
        if fieldType != saType || !sadefs.IsPreallocatedCSAFieldName(fieldName, fieldType) {
            continue
        }
        if _, ok := fieldToAliasMap[fieldName]; ok {
            cntUsed++
        } else {
            targetFieldName = fieldName
            break
        }
    }
    // ... update namespace config with alias mapping
}
```

## API Usage

### ListWorkflowExecutions

[service/frontend/workflow_handler.go](https://github.com/temporalio/temporal/blob/main/service/frontend/workflow_handler.go) (`WorkflowHandler.ListWorkflowExecutions`)

```go
func (wh *WorkflowHandler) ListWorkflowExecutions(
    ctx context.Context,
    request *workflowservice.ListWorkflowExecutionsRequest,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
    // ...
    req := &manager.ListWorkflowExecutionsRequestV2{
        NamespaceID:   namespaceID,
        Namespace:     namespaceName,
        PageSize:      int(request.GetPageSize()),
        NextPageToken: request.NextPageToken,
        Query:         request.GetQuery(),
    }
    persistenceResp, err := wh.visibilityMgr.ListWorkflowExecutions(ctx, req)
    // ...
}
```

### CountWorkflowExecutions

Similar to list, but returns counts. Supports `GROUP BY` for aggregations.

## CHASM vs Workflow Implementation Differences

CHASM (Coordinated Heterogeneous Application State Machines) is Temporal's framework for building new execution types beyond workflows.

### Namespace Division

CHASM uses `TemporalNamespaceDivision` to separate different archetype records in the same visibility table. Each CHASM archetype has a unique numeric ID.

[common/persistence/visibility/store/query/converter.go](https://github.com/temporalio/temporal/blob/main/common/persistence/visibility/store/query/converter.go) (`QueryConverter.Convert`)

```go
func (c *QueryConverter[ExprT]) Convert(queryString string) (*QueryParams[ExprT], error) {
    // ...
    if !c.seenNamespaceDivision {
        if c.archetypeID != chasm.UnspecifiedArchetypeID {
            // For CHASM queries, filter by archetype ID
            namespaceDivisionExpr, err = c.storeQC.ConvertComparisonExpr(
                sqlparser.EqualStr,
                nsDivisionCol,
                strconv.Itoa(int(c.archetypeID)),
            )
        } else {
            // For regular workflow queries, filter by null (no division)
            namespaceDivisionExpr, err = c.storeQC.ConvertIsExpr(
                sqlparser.IsNullStr,
                nsDivisionCol,
            )
        }
    }
    // ...
}
```

### Search Attribute Registration

| Aspect | Workflows | CHASM |
|--------|-----------|-------|
| Registration | Via OperatorService/AdminService API | Defined in component code |
| Storage | Custom columns or ES fields | `Temporal*` prefixed columns |
| Aliases | Namespace-scoped | Component-scoped |

CHASM components define their search attributes at compile time:

[chasm/lib/activity/activity.go](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/activity.go) (search attribute definitions)

```go
var (
    ActivityTypeSearchAttribute   = chasm.NewSearchAttributeKeyword(ActivityTypeSAAlias, chasm.SearchAttributeFieldKeyword01)
    TaskQueueSearchAttribute      = chasm.NewSearchAttributeKeyword(TaskQueueSAAlias, chasm.SearchAttributeFieldKeyword02)
    ActivityStatusSearchAttribute = chasm.NewSearchAttributeKeyword(ActivityStatusSAAlias, chasm.SearchAttributeFieldLowCardinalityKeyword01)
)
```

Components register these via the library:

[chasm/lib/activity/library.go](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/library.go) (`componentOnlyLibrary.Components`)

```go
func (l *componentOnlyLibrary) Components() []*chasm.RegistrableComponent {
    return []*chasm.RegistrableComponent{
        chasm.NewRegistrableComponent[*Activity]("activity",
            chasm.WithSearchAttributes(
                ActivityTypeSearchAttribute,
                TaskQueueSearchAttribute,
                ActivityStatusSearchAttribute,
            ),
        ),
    }
}
```

### Memo Structure

| Aspect | Workflows | CHASM |
|--------|-----------|-------|
| Format | `map[string]*Payload` | Typed proto message |
| Provider | SDK/user provided | Component implements `VisibilityMemoProvider` |

[chasm/visibility.go](https://github.com/temporalio/temporal/blob/main/chasm/visibility.go) (`VisibilityMemoProvider`)

```go
// VisibilityMemoProvider if implemented by the root Component,
// allows the CHASM framework to automatically determine, at the end of
// a transaction, if a visibility task needs to be generated to update the
// visibility record with the returned memo.
type VisibilityMemoProvider interface {
    Memo(Context) proto.Message
}
```

### Field Name Aliasing

CHASM archetypes can define aliases that map to system field names. For example, standalone activities use `ActivityId` as an alias for `WorkflowId`:

[common/persistence/visibility/store/query/resolve.go](https://github.com/temporalio/temporal/blob/main/common/persistence/visibility/store/query/resolve.go) (`ResolveSearchAttributeAlias`)

```go
// Handle ActivityId → WorkflowID transformation for standalone activities
if name == sadefs.ActivityId {
    saType, _ := saTypeMap.GetType(sadefs.WorkflowID)
    return sadefs.WorkflowID, saType, nil
}
```

## Limitations

1. **No JOINs** - Queries operate on a single table
2. **No subqueries** - Simple WHERE clauses only
3. **Pagination required** - No LIMIT clause; use `NextPageToken`
4. **ORDER BY** - Only supported with Elasticsearch (and deprecated)
5. **GROUP BY** - Single field only, primarily for `ExecutionStatus` aggregations
6. **System attribute aliasing** - System attributes like `TaskQueue` cannot be overridden by CHASM components (they bypass the CHASM mapper)



