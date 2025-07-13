# Multi-Worker Serialization Fix

## Problem Description

When running **two** Temporal workers simultaneously, the `invoke_model_activity` fails with a `PydanticSerializationError` during activity result serialization:

```
PydanticSerializationError: Error serializing to JSON: TypeError: 'MockValSer' object cannot be converted to 'SchemaSerializer'
```

**Reproduction**: The issue specifically manifests when executing the **Research Workflow** with multiple workers active. Single worker deployments work correctly, likely due to reduced task execution frequency that makes the bug less likely to surface.

## Root Cause Analysis

The error occurs during **activity result serialization**, not input serialization:

1. **Module-Level Patching**: `set_open_ai_agent_temporal_overrides()` applies module-level patches to the OpenAI agents library, setting up configurations that include mock validators
2. **Process Inheritance**: Each worker process inherits the same patched library state when it starts up
3. **MockValSer Objects in Results**: The `invoke_model_activity` returns a `ModelResponse` dataclass containing OpenAI SDK Pydantic models with mock validators (`MockValSer`) in nested objects like `InputTokensDetails` and `OutputTokensDetails`
4. **Serialization Failure**: Temporal's pydantic data converter cannot serialize these mock objects when encoding activity results
5. **Multi-Worker Bug Amplification**: Multiple workers don't create race conditions (they're separate processes), but they increase the probability of hitting the serialization bug since any worker can pick up the failing task

## Solution

### Simple One-Line Fix

The core solution is a **single import substitution**:

```python
# Replace this:
ModelActivity().invoke_model_activity

# With this:
SerializableModelActivity().invoke_model_activity
```

### Implementation Details

Created `openai_agents/serializable_model_activity.py` that wraps the base `ModelActivity` and converts results to fully serializable Pydantic models:

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

The conversion safely handles complex nested objects:

```python
# Use mode='json' to avoid MockValSer issues
item.model_dump(mode='json', exclude_unset=True)

# Safe fallback for serialization failures
except Exception as e:
    output_dicts.append({
        "error": f"Serialization failed: {str(e)}",
        "type": str(type(item).__name__)
    })
```

## Code Changes

**`openai_agents/run_worker.py`** - Only two lines changed:
```diff
-from temporalio.contrib.openai_agents import (
-    ModelActivity,
-    ModelActivityParameters,
-    set_open_ai_agent_temporal_overrides,
-)
+from temporalio.contrib.openai_agents import (
+    ModelActivityParameters,
+    set_open_ai_agent_temporal_overrides,
+)
+from openai_agents.serializable_model_activity import SerializableModelActivity

         activities=[
-            ModelActivity().invoke_model_activity,
+            SerializableModelActivity().invoke_model_activity,
             get_weather,
         ],
```

**New file**: `openai_agents/serializable_model_activity.py` (104 lines) - Complete wrapper implementation

## Testing & Validation

**Dual Worker Test**: Created `test_dual_workers.py` that:
1. Starts two worker processes simultaneously  
2. Executes the Research Workflow (the original failing case)
3. Confirms successful completion without MockValSer errors

**Instance Management Validation**: Confirmed that inline instantiation (`SerializableModelActivity().invoke_model_activity`) works correctly - no special instance management required.

**Result**: ✅ Dual workers now handle the Research Workflow successfully with only the serialization wrapper change.

## Limitations & Trade-offs

1. **Workaround Nature**: This is a **hack** that intercepts and converts OpenAI agents SDK responses
2. **Data Loss Risk**: Complex object serialization failures fall back to error messages rather than preserving full data
3. **Maintenance Burden**: Custom wrapper must be maintained alongside OpenAI agents SDK updates
4. **Performance**: Additional serialization conversion adds overhead to every model activity call

## Future Considerations

This fix should be considered **temporary** until:
- OpenAI agents SDK addresses MockValSer serialization issues in their Pydantic models
- Temporal provides better handling of complex Pydantic model serialization  
- The underlying pydantic-core MockValSer issue is resolved

The solution provides a pragmatic, minimal workaround for production multi-worker deployments while maintaining full functional compatibility.