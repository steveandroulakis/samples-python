# Claude Code Assistant Guide for OpenAI Agents SDK

## Project Structure

This is the OpenAI Agents SDK Python samples project located at:
- **Base directory**: `/Users/steveandroulakis/Code/openai-agents-sdk/samples-python`
- **Virtual environment**: `.venv` (already configured with uv)
- **Main workflows**: `openai_agents/workflows/`
- **Worker script**: `openai_agents/run_worker.py`
- **Sample workflow runners**: `openai_agents/run_*_workflow.py`

## Key Directories

- `openai_agents/workflows/` - Contains all workflow implementations
  - `research_agents/` - Research workflow components (planner, search, writer agents)
  - `agents_as_tools_workflow.py` - Multi-agent orchestration example
  - `customer_service_workflow.py` - Customer service agent example
  - `hello_world_workflow.py` - Simple haiku-generating agent
  - `tools_workflow.py` - Tool usage example

- `openai_agents/` - Main package with workflow runners
  - `run_worker.py` - Temporal worker that handles all workflows
  - `run_research_workflow.py` - Research workflow client
  - `run_hello_world_workflow.py` - Hello world workflow client

## Running Workflows with Workers

### Environment Setup
The project uses `uv` for dependency management. Always ensure dependencies are synced:

```bash
uv sync --group openai-agents
```

### Standard Workflow Execution Pattern

For any workflow, use this pattern (adjust timeout based on workflow complexity):

```bash
# Start worker in background
uv run openai_agents/run_worker.py &
WORKER_PID=$!
echo "Worker started with PID: $WORKER_PID"

# Wait for worker initialization
sleep 5

# Run the workflow (adjust timeout as needed)
echo "Running [workflow_name] workflow..."
timeout [SECONDS] uv run openai_agents/run_[workflow_name]_workflow.py

# Clean up worker
echo "Workflow completed, stopping worker..."
kill $WORKER_PID 2>/dev/null || true
wait $WORKER_PID 2>/dev/null || true
echo "Worker stopped"
```

### Specific Workflow Examples

#### Research Workflow (Takes ~2-3 minutes)
```bash
uv run openai_agents/run_worker.py &
WORKER_PID=$!
echo "Worker started with PID: $WORKER_PID"
sleep 5
echo "Running research workflow..."
timeout 180 uv run openai_agents/run_research_workflow.py
echo "Workflow completed, stopping worker..."
kill $WORKER_PID 2>/dev/null || true
wait $WORKER_PID 2>/dev/null || true
echo "Worker stopped"
```

#### Hello World Workflow (Quick test)
```bash
uv run openai_agents/run_worker.py &
WORKER_PID=$!
echo "Worker started with PID: $WORKER_PID"
sleep 2
echo "Running hello world workflow..."
uv run openai_agents/run_hello_world_workflow.py
echo "Workflow completed, stopping worker..."
kill $WORKER_PID 2>/dev/null || true
wait $WORKER_PID 2>/dev/null || true
echo "Worker stopped"
```

### Workflow Timeouts by Type

- **Hello World**: 30 seconds (simple haiku generation)
- **Tools Workflow**: 60 seconds (single tool usage)
- **Research Workflow**: 180 seconds (multiple web searches + report generation)
- **Customer Service**: 120 seconds (conversation handling)
- **Agents as Tools**: 240 seconds (complex multi-agent orchestration)

### Worker Management

#### Kill All Workers (if something goes wrong)
```bash
pkill -f "run_worker.py" || true
```

#### Check for Running Workers
```bash
ps aux | grep "run_worker.py" | grep -v grep || echo "No worker processes found"
```

## Common Issues and Solutions

### Serialization Errors
If you see `PydanticSerializationError` with `MockValSer` objects:
- Check `openai_agents/workflows/research_agents/research_manager.py` 
- Ensure return values are plain strings, not objects with mock components
- See `python-serialization-error.md` for detailed troubleshooting

### Worker Timeout Issues
- Increase timeout values for complex workflows
- Research workflow typically needs 180+ seconds
- Multi-agent workflows may need 240+ seconds

### Environment Issues
- Always run from the base directory: `/Users/steveandroulakis/Code/openai-agents-sdk/samples-python`
- Ensure virtual environment is activated: `source .venv/bin/activate` (though uv run handles this)
- Check dependencies: `uv sync --group openai-agents`

## Testing Approach

When testing workflow changes:
1. Start with hello world to verify basic setup
2. Test specific workflow with appropriate timeout
3. Monitor output for serialization errors
4. Clean up workers between tests

## Key Files for Debugging

- `openai_agents/run_worker.py` - Worker configuration and activity registration
- `openai_agents/workflows/research_agents/research_manager.py` - Main research logic
- `python-serialization-error.md` - Serialization troubleshooting guide
- `openai_agents/QUICKSTART.md` - Basic usage examples