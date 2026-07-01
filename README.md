# Project Template

Template repository for initializing new GitHub projects within the organization.

## Module Structure

- `app/` - Main application executable.
- `lib/` - Shared library code and core logic.
- `bench/` - Benchmarking suite for evaluating shared functions.
- `buildinfo/` - Build metadata and versioning configuration.

## Build & Execution

This project utilizes Gradle as its build system.

```bash
# Build all modules and execute tests
./gradlew build

# Run the main application
./gradlew :app:run

# Execute benchmarking suite
./gradlew :bench:benchmark
```

## Continuous Integration

Continuous integration is enforced via GitHub Actions. Push and pull request events trigger automated builds, tests, and
formatting checks.

## Contributing

Refer to [CONTRIBUTING.md](https://github.com/CollaborativeStateMachines/.github/blob/main/CONTRIBUTING.md) for strict
organizational guidelines regarding code style, commit
conventions, and the review process.