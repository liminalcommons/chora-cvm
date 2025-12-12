@build-governance
Feature: Build Governance
  As a developer
  I want quality gates to catch issues at commit time
  So that bugs never reach CI or production

  Background:
    Given a temporary test database

  @behavior:behavior-lint-passes
  Scenario: Lint passes on clean code
    Given a Python package with valid code
    When ruff check is executed
    Then the lint exit code is 0
    And no lint violations are reported

  @behavior:behavior-types-check
  Scenario: Type check passes on typed code
    Given a Python package with type hints
    When mypy is executed
    Then the typecheck exit code is 0
    And no type errors are reported

  @behavior:behavior-tests-pass
  Scenario: Tests pass on working code
    Given a package with a passing test suite
    When pytest is executed
    Then all tests pass
    And the test exit code is 0

  @behavior:behavior-coverage-threshold
  Scenario: Coverage threshold is met
    Given a package with pytest-cov configured
    When tests complete with coverage measurement
    Then coverage percentage meets threshold

  @behavior:behavior-security-scan-clean
  Scenario: Security scan finds no issues
    Given a Python package with secure code
    When bandit is executed
    Then no security vulnerabilities are detected

  @behavior:behavior-build-integrity-check
  Scenario: Build integrity check runs all checks
    Given a workspace with Python packages
    When check_build_integrity is called
    Then results are aggregated for each package
    And the overall health status is reported
