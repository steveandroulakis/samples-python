# OpenAI Agents Quick Start

## Running the Hello World Sample

To run the hello world sample with automatic worker management:

```bash
# ensure this is run once
uv sync --group openai-agents
```

```bash
source .venv/bin/activate
uv run openai_agents/run_worker.py &
WORKER_PID=$!
echo "Worker started with PID: $WORKER_PID"
sleep 2
echo "Running hello world workflow..."
uv run openai_agents/run_hello_world_workflow.py
echo "Workflow completed, stopping worker..."
kill $WORKER_PID
wait $WORKER_PID 2>/dev/null || true
echo "Worker stopped"
```

This command will:
1. Start the worker in the background
2. Wait 2 seconds for the worker to initialize
3. Execute the hello world workflow
4. Stop the worker after completion

Expected output: A haiku response about the provided prompt.