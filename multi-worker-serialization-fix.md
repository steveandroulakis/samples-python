# Multi-Worker Serialization Fix

## Problem Description

When running **two** Temporal workers simultaneously, the `invoke_model_activity` fails with a `PydanticSerializationError` during activity result serialization:

```
PydanticSerializationError: Error serializing to JSON: TypeError: 'MockValSer' object cannot be converted to 'SchemaSerializer'
```

**Reproduction**: The issue specifically manifests when executing the **Research Workflow** with multiple workers active. Single-worker deployments work correctly, even with parallel activity execution, because the serialization bug only surfaces across process boundaries.

## Root Cause Analysis

The error occurs during **activity result serialization**, not input serialization. Here's the detailed technical breakdown:

### 1. Module-Level Patching Infrastructure

**Location**: `temporalio/contrib/openai_agents/temporal_openai_agents.py:32-84` (Temporal Python SDK)

The `set_open_ai_agent_temporal_overrides()` context manager establishes the patching infrastructure:

```python
@contextmanager
def set_open_ai_agent_temporal_overrides(
    model_params: Optional[ModelActivityParameters] = None,
    auto_close_tracing_in_workflows: bool = False,
):
```

This function applies **two critical module-level patches**:
- **Agent Runner Override**: `set_default_agent_runner(TemporalOpenAIRunner(model_params))`
- **Trace Provider Override**: `set_trace_provider(TemporalTraceProvider())`

### 2. Model Interception and Patching Chain

**Architecture Flow**: 
```
OpenAI Agent → TemporalOpenAIRunner → _TemporalModelStub → ModelActivity.invoke_model_activity
```

**Key Files** (Temporal Python SDK):
- `temporalio/contrib/openai_agents/_openai_runner.py:64-68` - Replaces `run_config.model` with `_TemporalModelStub`
- `temporalio/contrib/openai_agents/_temporal_model_stub.py:154-167` - Converts model calls to `workflow.execute_activity_method()`
- `temporalio/contrib/openai_agents/_invoke_model_activity.py:128-184` - The actual activity implementation

### 3. MockValSer Object Injection

**CONFIRMED**: The patching mechanism injects `MockValSer` objects into nested OpenAI SDK model components:
- **InputTokensDetails** - Token usage tracking objects
- **OutputTokensDetails** - Response token metadata
- **Nested Usage Objects** - Within `ModelResponse.usage` structures

These objects are **Pydantic validator artifacts** that are not JSON serializable.

### 4. Serialization Failure Point

**CONFIRMED** - **Exact Location**: `temporalio/worker/_activity.py:460` (Temporal Python SDK)

```python
result = await self._execute_activity(start, running_activity, task_token)
[payload] = await self._data_converter.encode([result])  # <-- FAILS HERE
completion.result.completed.result.CopyFrom(payload)
```

**CONFIRMED** - **Serialization Chain**:
1. `DataConverter.encode()` → `PydanticJSONPlainPayloadConverter.to_payload()`
2. `to_payload()` calls `pydantic_core.to_json(value)`
3. `pydantic_core.to_json()` encounters `MockValSer` and fails

**CONFIRMED** - **Error**: `TypeError: 'MockValSer' object cannot be converted to 'SchemaSerializer'`

### 5. Multi-Worker vs Single-Worker Process Boundaries

**CONFIRMED** - **Multi-Worker Environment**:
- Each worker runs in its **own process**
- Activity results must be **fully serialized** to send to Temporal Server
- **Process boundary** triggers complete JSON serialization via `pydantic_data_converter`

**CONFIRMED** - **Single-Worker Environment**:
- All operations within **single process**
- **No process boundary** = no full serialization requirement
- MockValSer objects remain in-memory and don't trigger serialization failure

### 6. Why This Happens in Multi-Worker Mode

**CONFIRMED**: Multiple workers introduce process-level isolation.

**HYPOTHESIS**: While each worker may independently apply `set_open_ai_agent_temporal_overrides()`, inconsistencies in patch timing or lazy initialization of model schemas can cause some model classes to include unserializable artifacts like `MockValSer`.

**CONFIRMED**: In a multi-worker setup:

* **Activity results must be serialized across process boundaries**.
* **Temporal SDK serializes the result using a data converter**, which fails if any unserializable object (e.g. `MockValSer`) remains.

## Solution

### Simple One-Line Fix

The core solution is a **single import substitution**:

```python
# Replace this:
ModelActivity().invoke_model_activity

# With this:
SerializableModelActivity().invoke_model_activity
```

### Fix Explanation

`SerializableModelActivity` is a wrapper around the original `ModelActivity` that ensures all returned model data is **converted into fully serializable Pydantic objects**, avoiding any leaking `MockValSer` instances.

#### Implementation Highlights

**Location**: `openai_agents/serializable_model_activity.py:94-104` (samples repo)

```python
class SerializableModelActivity(BaseModelActivity):
    """ModelActivity wrapper that returns serializable responses."""

    @activity.defn
    async def invoke_model_activity(self, input: ActivityModelInput) -> SerializableModelResponse:
        # Call parent implementation to get ModelResponse
        response = await super().invoke_model_activity(input)

        # Convert to serializable format
        return SerializableModelResponse.from_model_response(response)
```

**Key Serialization Strategy** (`openai_agents/serializable_model_activity.py:66`, samples repo):

```python
# Use mode='json' to avoid MockValSer issues
output_dicts.append(item.model_dump(mode='json', exclude_unset=True))
```

**Safe Fallback for MockValSer Objects** (`openai_agents/serializable_model_activity.py:73-78`, samples repo):

```python
# Safe fallback for serialization failures
except Exception as e:
    output_dicts.append({
        "error": f"Serialization failed: {str(e)}",
        "type": str(type(item).__name__)
    })
```

**Usage Conversion** (`openai_agents/serializable_model_activity.py:18-47`, samples repo):
The `SerializableUsage.from_usage()` method safely extracts data from potentially contaminated usage objects using multiple fallback strategies for `input_tokens_details` and `output_tokens_details`.

### Code Changes

The core integration in `openai_agents/run_worker.py` (samples repo) only involves a small substitution, but it depends on a new module for the serialization fix logic:

```diff
- from temporalio.contrib.openai_agents import (
-     ModelActivity,
-     ModelActivityParameters,
-     set_open_ai_agent_temporal_overrides,
- )
+ from temporalio.contrib.openai_agents import (
+     ModelActivityParameters,
+     set_open_ai_agent_temporal_overrides,
+ )
+ from openai_agents.serializable_model_activity import SerializableModelActivity

         activities=[
-            ModelActivity().invoke_model_activity,
+            SerializableModelActivity().invoke_model_activity,
             get_weather,
         ],
```

**New file**: `openai_agents/serializable_model_activity.py` (samples repo) – Adds a dedicated 100+ line wrapper to handle proper serialization of nested response data. While the change in the main worker file is minimal, this new module contains the real fix logic and must be maintained alongside the SDK.

## Testing & Validation

### Dual Worker Test

**Location**: `test_dual_workers.py` (samples repo)

Created comprehensive test that:

1. **Starts two worker processes simultaneously** (`test_dual_workers.py:44-62`)
2. **Executes the Research Workflow** - the original failing case (`test_dual_workers.py:17-41`)
3. **Validates MockValSer error detection** (`test_dual_workers.py:39-40`)
4. **Confirms successful completion** without serialization errors

**Test Workflow**: Uses `ResearchWorkflow.run` which triggers the complex model interactions that originally caused the MockValSer serialization failure.

**Validation Approach**: The test specifically checks for "MockValSer" strings in exception messages to ensure we're detecting the exact issue being fixed.

### Instance Management Validation

Confirmed that inline instantiation (`SerializableModelActivity().invoke_model_activity`) works correctly — no special lifecycle management required.

### Result: Success

Dual workers now handle the Research Workflow correctly with only the serialization wrapper change.

## Limitations & Trade-offs

1. **Workaround Nature**: This is a **shim** that adapts OpenAI agents SDK responses into serializable forms.
2. **Data Loss Risk**: If objects can't be serialized, fallback output includes only error metadata, not full original data.
3. **Maintenance Burden**: Custom wrapper must track changes in the OpenAI agents SDK.
4. **Performance Cost**: Additional conversion step on every result adds some latency.

## Future Considerations

This fix should be treated as **temporary** until one of the following occurs:

* The OpenAI agents SDK provides serialization-safe models by default
* Temporal provides a serialization mode tolerant of custom validator artifacts
* Pydantic-core or OpenAI’s patching avoids emitting `MockValSer` in production paths
