# Project Workflow

## Guiding Principles

1. **The Plan is the Source of Truth:** All work must be tracked in `plan.md`
2. **The Tech Stack is Deliberate:** Changes to the tech stack must be documented in `tech-stack.md` *before* implementation
3. **Test-Driven Development:** Write unit tests before implementing functionality
4. **Target Code Coverage:** Aim for >40% code coverage for all modules
5. **User Experience First:** Every decision should prioritize user experience
6. **Non-Interactive & CI-Aware:** Prefer non-interactive commands. Use `CI=true` for watch-mode tools (tests, linters) to ensure single execution.

## Task Workflow

All tasks follow a strict lifecycle:

### Standard Task Workflow

1. **Select Task:** Choose the next available task from `plan.md` in sequential order

2. **Mark In Progress:** Before beginning work, edit `plan.md` and change the task from `[ ]` to `[~]`

3. **Write Failing Tests (Red Phase):**
   - Create a new test file for the feature or bug fix.
   - Write one or more unit tests that clearly define the expected behavior and acceptance criteria for the task.
   - **CRITICAL:** Run the tests and confirm that they fail as expected. This is the "Red" phase of TDD. Do not proceed until you have failing tests.

4. **Implement to Pass Tests (Green Phase):**
   - Write the minimum amount of application code necessary to make the failing tests pass.
   - Run the test suite again and confirm that all tests now pass. This is the "Green" phase.

5. **Refactor (Optional but Recommended):**
   - With the safety of passing tests, refactor the implementation code and the test code to improve clarity, remove duplication, and enhance performance without changing the external behavior.
   - Rerun tests to ensure they still pass after refactoring.

6. **Verify Coverage & Quality:**
   - Run coverage reports using the project's chosen tools.
   - **CRITICAL:** Run `pre-commit run --all-files` and address any errors that appear.
   - Target: >40% coverage for new code.

7. **Document Deviations:** If implementation differs from tech stack:
   - **STOP** implementation
   - Update `tech-stack.md` with new design
   - Add dated note explaining the change
   - Resume implementation

8. **Manual Completion:**
   - **Note:** Commits are handled manually by the user.
   - Update `plan.md` status from `[~]` to `[x]` once the task is fully verified.

### Phase Completion Verification and Checkpointing Protocol

**Trigger:** This protocol is executed immediately after a task is completed that also concludes a phase in `plan.md`.

1.  **Announce Protocol Start:** Inform the user that the phase is complete and the verification protocol has begun.

2.  **Ensure Test Coverage for Phase Changes:**
    -   Verify that all new code has corresponding tests and coverage meets the >40% requirement.
    -   Run `pre-commit run --all-files` to ensure all files meet quality standards.

3.  **Execute Automated Tests with Proactive Debugging:**
    -   Execute the full test suite.
    -   If tests fail, begin debugging. Attempt to propose a fix a **maximum of two times**. If failure persists, stop and ask the user for guidance.

4.  **Propose a Detailed, Actionable Manual Verification Plan:**
    -   Analyze `product.md`, `product-guidelines.md`, and `plan.md` to determine phase goals.
    -   Generate a step-by-step manual verification plan for the user.

5.  **Await Explicit User Feedback:**
    -   Pause and await the user's response to the verification plan.

6.  **Manual Checkpoint:**
    -   The user handles all commits and checkpoints manually.
    -   Update `plan.md` status to mark the phase as complete once verified.

### Quality Gates

Before marking any task complete, verify:

- [ ] All tests pass
- [ ] Code coverage meets requirements (>40%)
- [ ] Code follows project's code style guidelines (as defined in `code_styleguides/`)
- [ ] No linting or static analysis errors (via `pre-commit`)
- [ ] Documentation updated if needed
- [ ] No security vulnerabilities introduced

## Development Commands

### Setup
```bash
uv sync --extra dev
pre-commit install
```

### Daily Development
```bash
# Run tests
pytest

# Run linting/formatting
ruff check .
ruff format .

# Run pre-commit checks
pre-commit run --all-files
```

### Before Marking Task Complete
```bash
# Run tests with coverage
pytest --cov=src --cov-report=term-missing

# Final quality check
pre-commit run --all-files
```

## Testing Requirements

### Unit Testing
- Every module must have corresponding tests.
- Mock external dependencies (e.g., Databricks API calls).
- Test both success and failure cases.

### Integration Testing
- Test complete flows on Databricks using Databricks Connect where appropriate.
- Verify RAG retrieval and Agent tool usage.

## Commit Guidelines (Manual)

Users are encouraged to follow clear and concise commit messages.

### Message Format
```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

### Types
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Formatting, missing semicolons, etc.
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `test`: Adding missing tests
- `chore`: Maintenance tasks

## Definition of Done

A task is complete when:

1. All code implemented to specification
2. Unit tests written and passing
3. Code coverage meets project requirements (>40%)
4. `pre-commit` checks pass without errors
5. Documentation complete (if applicable)
6. Task marked as `[x]` in `plan.md`

## Emergency Procedures

### Critical Bug in Production
1. Create hotfix branch
2. Write failing test
3. Implement minimal fix
4. Test thoroughly
5. Document in plan.md

## Deployment Workflow (via DABs)

### Pre-Deployment Checklist
- [ ] All tests passing
- [ ] Coverage >40%
- [ ] No linting errors (`pre-commit`)
- [ ] Environment variables configured
- [ ] Backup/Checkpoint created

### Deployment Steps
1. Deploy to dev target: `databricks bundle deploy -t dev`
2. Verify deployment in Databricks Workspace
3. Merge to main for production deployment (handled via CI/CD)
