# Custom Search Attributes for Standalone Activities

## Feature Overview

Users can:
1. Register custom search attributes for their namespace (e.g., `ActivityCustomKeyword`)
2. Start standalone activities with custom search attribute values
3. Query activities using those custom search attributes

## Functional Tests

The following tests would fail without the unalias/restore logic:

- `TestListActivityExecutions/QueryByCustomSearchAttribute`
- `TestCountActivityExecutions/CountByCustomSearchAttribute`

These tests:
1. Register a custom search attribute via `OperatorClient().AddSearchAttributes()`
2. Start an activity with that custom SA
3. Query for the activity using the custom SA name

## The Problem

When a user registers a custom search attribute like `ActivityCustomKeyword`, Temporal creates an internal mapping:

```
ActivityCustomKeyword → Keyword08
```

The user-friendly name (`ActivityCustomKeyword`) is called the **alias**, and the internal field (`Keyword08`) is the **field name**.

### Two Components with Different Expectations

**1. The Search Attribute Validator**

[common/searchattribute/validator.go (`Validator.Validate`)](https://github.com/temporalio/temporal/blob/main/common/searchattribute/validator.go#L60)

```go
// Validate search attributes are valid for writing.
// The search attributes must be unaliased before calling validation.
func (v *Validator) Validate(searchAttributes *commonpb.SearchAttributes, namespace string) error {
```

The validator looks up field types using the field name (e.g., `Keyword08`), not the alias. If you pass an alias like `ActivityCustomKeyword`, validation fails because it's not in the type map.

**2. The CHASM Visibility Executor**

[service/history/visibility_queue_task_executor.go](https://github.com/temporalio/temporal/blob/main/service/history/visibility_queue_task_executor.go#L408-L418)

```go
aliasedCustomSearchAttributes := visComponent.GetSearchAttributes(visTaskContext)
for alias, value := range aliasedCustomSearchAttributes {
    fieldName, err := customSaMapper.GetFieldName(alias, namespaceEntry.Name().String())
    if err != nil {
        t.logger.Warn("Failed to get field name for alias, ignoring search attribute", ...)
        continue
    }
    searchattributes[fieldName] = value
}
```

The CHASM visibility executor expects search attributes to be stored with their **alias** names. It then converts aliases to field names when writing to the visibility store.

### The Conflict

| Component | Expects |
|-----------|---------|
| Validator | Field names (e.g., `Keyword08`) |
| CHASM Visibility | Aliases (e.g., `ActivityCustomKeyword`) |

If we store with field names (as workflows do), the visibility executor's `GetFieldName("Keyword08", namespace)` fails because `Keyword08` is not an alias.

## The Solution

[chasm/lib/activity/frontend.go (`frontendHandler.validateAndPopulateStartRequest`)](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/frontend.go#L328-L352)

```go
// TODO: Unalias for validation, then restore aliased SA for CHASM visibility storage. The
// validator requires unaliased format but CHASM visibility expects aliased format.
originalSA := req.SearchAttributes
if originalSA != nil {
    unaliasedSA, err := searchattribute.UnaliasFields(
        h.saMapperProvider,
        originalSA,
        req.GetNamespace(),
    )
    if err != nil {
        return nil, err
    }
    req.SearchAttributes = unaliasedSA
}

err = validateAndNormalizeStartActivityExecutionRequest(...)

req.SearchAttributes = originalSA  // Restore aliased for CHASM storage
```

**Flow:**
1. Save the original aliased search attributes
2. Unalias them (convert `ActivityCustomKeyword` → `Keyword08`)
3. Validate with unaliased format
4. Restore the original aliased format for CHASM storage

## Data Flow Diagram

```
User Request
    │
    │  SearchAttributes: { "ActivityCustomKeyword": "value" }
    ▼
┌─────────────────────────────────────────────────────┐
│  Frontend Handler                                   │
│                                                     │
│  1. Save original SA (aliased)                      │
│  2. Unalias: ActivityCustomKeyword → Keyword08      │
│  3. Validate (needs field names)                    │
│  4. Restore original SA (aliased)                   │
└─────────────────────────────────────────────────────┘
    │
    │  SearchAttributes: { "ActivityCustomKeyword": "value" }
    ▼
┌─────────────────────────────────────────────────────┐
│  CHASM Storage                                      │
│                                                     │
│  Stores SA with alias names                         │
└─────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────┐
│  Visibility Queue Task Executor                     │
│                                                     │
│  Reads SA, converts alias → field name              │
│  ActivityCustomKeyword → Keyword08                  │
│  Writes to visibility store with field names        │
└─────────────────────────────────────────────────────┘
```

## Future Work

The CHASM visibility team plans to refactor this so that CHASM stores unaliased search attributes (like workflows do), which would eliminate the need for this workaround.

