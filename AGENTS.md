# Repository Guidelines

## Hierarchical Agent Guidelines

This is the **root AGENTS.md** containing universal guidelines that apply to the entire codebase. Each module can have its own `AGENTS.md` that inherits these principles and adds module-specific conventions.

## Universal Code Quality & Architecture Principles

### Fail-Fast Philosophy
- No fallbacks: fail fast on missing dependencies or invalid states.
- Surface explicit exceptions rather than silent defaults.
- Remove dead code immediately - don't keep "just in case" functions.
- Fail fast on missing required dependencies - don't create partially functional objects.

### Clean Architecture
- Keep components loosely coupled for easier testing and dependency injection.
- Use dependency injection (DI) to provide dependencies to classes.
- Avoid static classes and singletons in favor of injectable services.
- Adhere to the principle of separation of concerns. For example, low-level code (like `Sparrow`) should not contain any application-specific logic.

## Universal Error Handling & Messages

### Exception Patterns
- Throw specific exceptions instead of generic `Exception`.
- Use `try-catch` blocks to handle exceptions gracefully.
- Don't swallow exceptions; either handle them or let them propagate.
- When catching and re-throwing, wrap the original exception to preserve the stack trace.

## Universal Testing Philosophy

### Test Invariants Over Constants
- Prefer testing relationships over constants.
- Test public behavior and invariants, not implementation details.

### Minimize External Dependencies in Tests
- Minimize mocks/fakes; use them only when determinism cannot be achieved otherwise.
- Prefer integration tests against real dependencies when needed.
- Test critical paths including error handling scenarios.

### Test Categorization
- Use categories to distinguish between different types of tests (e.g., unit, integration, performance).
- Test categorization should be based on the functionality being tested, not the directory location.

## Universal Naming & Style Conventions

### Code Style
- Follow the [.NET coding conventions](https://docs.microsoft.com/en-us/dotnet/csharp/fundamentals/coding-style/coding-conventions) and the rules defined in the project's `.editorconfig` file.
- Use `PascalCase` for classes, methods, and properties.
- Use `_camelCase` for private and internal fields.
- Use `var` sparingly, only when the type is not obvious from the right-hand side of the assignment.
- Keep public APIs documented with XML comments.
- **Boolean Negation:** Prefer `== false` over `!` for boolean negation to improve readability and consistency.

## Performance Considerations
- Be mindful of performance, especially in performance-sensitive areas.
- Code marked with `PERF` is performance-critical and should not be modified without careful consideration and benchmarking.
- Avoid unnecessary allocations in hot paths. Use `structs`, `Span<T>`, and other performance-oriented features of C# where appropriate.

## Data Prioritization

- **SNMP Data Priority:** When conflicting or overlapping data sources exist (e.g., SNMP vs. other fallback metrics), prioritize SNMP information. SNMP data is generally considered more reliable for system-level metrics, and fallback methods should be treated as less reliable.

## Project Structure & Module Organization
- `src/RavenBench/`: The main benchmark project.
  - `Cli/`: Command-line parsing and handling.
  - `Workload/`: Defines different benchmark workloads.
  - `Transport/`: Communication with RavenDB.
  - `Metrics/`: Metrics collection and recording.
  - `Reporting/`: Results reporting (e.g., CSV, JSON).
- `tests/RavenBench.Tests/`: The test project for `RavenBench`.

## Build, Test, and Development Commands
- Build the solution: `dotnet build RavenBench.sln`
- Run the benchmarks: `dotnet run --project src/RavenBench/RavenBench.csproj -- [options]`
- Run the tests: `dotnet test tests/RavenBench.Tests/RavenBench.Tests.csproj`
