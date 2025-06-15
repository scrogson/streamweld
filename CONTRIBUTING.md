# Contributing to StreamWeld

Thank you for your interest in contributing to StreamWeld! This document provides guidelines and information for contributors.

## Development Setup

### Prerequisites

- Rust 1.70.0 or later
- Git

### Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally:

   ```bash
   git clone https://github.com/YOUR_USERNAME/streamweld.git
   cd streamweld
   ```

3. Create a new branch for your feature or fix:

   ```bash
   git checkout -b feature/your-feature-name
   ```

4. Make your changes and test them:
   ```bash
   cargo test --all-features
   cargo run --example basic
   ```

## Development Workflow

### Code Quality

We maintain high code quality standards:

- **Formatting**: Code must be formatted with `rustfmt`

  ```bash
  cargo fmt --all
  ```

- **Linting**: Code must pass `clippy` without warnings

  ```bash
  cargo clippy --all-targets --all-features -- -D warnings
  ```

- **Testing**: All tests must pass
  ```bash
  cargo test --all-features
  cargo test --doc
  ```

### Continuous Integration

Our CI pipeline runs on every pull request and includes:

- **Multi-platform testing**: Ubuntu, Windows, and macOS
- **Multiple Rust versions**: Stable, beta, and nightly
- **Code coverage**: Tracked via Codecov
- **Security auditing**: Dependency vulnerability scanning
- **Documentation**: Ensures docs build correctly

- **MSRV checking**: Minimum Supported Rust Version validation

### Making Changes

1. **Write tests first**: For new features, write tests before implementation
2. **Update documentation**: Keep docs in sync with code changes
3. **Add examples**: For significant features, add usage examples
4. **Consider performance**: For performance-critical changes, test manually

### Testing

Run the full test suite:

```bash
# Unit and integration tests
cargo test --all-features

# Documentation tests
cargo test --doc

# Run examples to ensure they work
cargo run --example basic
cargo run --example complex
cargo run --example examples

# Run benchmarks (if you have them set up locally)
# cargo bench
```

### Documentation

- Update relevant documentation for any API changes
- Add doc comments for new public APIs
- Update examples if behavior changes
- Consider adding new examples for significant features

## Submitting Changes

### Pull Request Process

1. **Ensure CI passes**: All checks must be green
2. **Write clear commit messages**: Use conventional commit format when possible
3. **Update CHANGELOG.md**: Add entry for your changes
4. **Request review**: Tag maintainers for review

### Commit Message Format

We prefer conventional commit messages:

```
type(scope): description

[optional body]

[optional footer]
```

Types:

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

Examples:

```
feat(sources): add MergeSource for combining multiple sources
fix(pipeline): handle empty batches correctly in demand processing
docs(readme): update examples with new GenStage-style API
```

### Pull Request Template

When creating a pull request, please include:

- **Description**: What does this PR do?
- **Motivation**: Why is this change needed?
- **Testing**: How was this tested?
- **Breaking changes**: Any API changes?
- **Related issues**: Link to relevant issues

## Code Style

### Rust Guidelines

- Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- Use `rustfmt` with default settings
- Prefer explicit types when it improves clarity
- Use meaningful variable and function names
- Add doc comments for all public APIs

### Architecture Principles

- **Demand-driven**: Maintain explicit demand signaling
- **Batch-first**: Prioritize batch processing for efficiency
- **Zero-copy**: Minimize allocations where possible
- **Async-native**: Built for Rust's async ecosystem
- **Type safety**: Leverage Rust's type system

### Error Handling

- Use `Result<T, Error>` for fallible operations
- Provide meaningful error messages
- Use `thiserror` for custom error types
- Document error conditions in doc comments

## Release Process

Releases are handled by maintainers:

1. Version bump in `Cargo.toml`
2. Update `CHANGELOG.md`
3. Create git tag: `git tag v0.x.y`
4. Push tag: `git push origin v0.x.y`
5. GitHub Actions automatically publishes to crates.io

## Getting Help

- **Issues**: Open an issue for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions
- **Discord**: Join our community Discord (link in README)

## Code of Conduct

This project follows the [Rust Code of Conduct](https://www.rust-lang.org/policies/code-of-conduct). Please be respectful and inclusive in all interactions.

## Recognition

Contributors are recognized in:

- `CHANGELOG.md` for their contributions
- GitHub contributors page
- Special thanks in release notes for significant contributions

Thank you for contributing to StreamWeld!
