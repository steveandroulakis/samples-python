# Branch Summary: openai-agents vs main

The main differences between the `openai-agents` branch and `main` are:

## Configuration Changes
- Added `.python-version` file specifying Python 3.10.16
- Updated `pyproject.toml` to require Python ~3.10.16 (was ~3.9) - Required due to Python 3.10+ union type syntax (`str | None`) in `customer_service_workflow.py` causing `TypeError: unsupported operand type(s) for |` on Python 3.9
- Updated `uv.lock` file with Python 3.10.16 dependencies

## Documentation Added
- `CLAUDE.md` - Comprehensive guide for Claude Code Assistant with project structure, workflow execution patterns, timeouts, and troubleshooting
- `openai_agents/QUICKSTART.md` - Simple hello world workflow execution guide
- `python-serialization-error.md` - Detailed debugging guide for Temporal serialization issues

## Critical Bug Fix
- Fixed serialization error in `research_manager.py:69` that was causing the research workflow to fail
- The issue was `MockValSer` objects from OpenAI Agents SDK couldn't be serialized by Temporal's Pydantic converter
- Added proper text extraction logic to ensure only plain strings are returned from the `_search()` method

## Summary
The branch essentially represents Steve's fork with Python version updates, comprehensive documentation for Claude Code usage, and a critical bug fix for the research workflow serialization issue.