
# Instructions for AI agent

## Code base

This repository is a Python project, which dependencies are managed with uv.

Code organization:

- `src` directory: main code
- `tests` directory: tests handled by `pytest`
- `docs` directory: documentation

Strictly follow data models and structures.

## Code style

When writing code:

- Follow PEP 8 and PEP 257
- Keep code consistent, easy to understand yet pythonic
- Add meaningful variable names
- Add explanatory comments describing the purpose of the code

When writing functions, always:

- Add descriptive docstrings.
- Use early returns for error conditions

Never import libraries by yourself. Always ask before adding dependencies.

## Code review

When reviewing code, focus on:

### Security issues

- Check for hardcoded secrets, API keys, or credentials
- Look for SQL injection and XSS vulnerabilities
- Verify proper input validation and sanitization

### Performance red flags

- Spot inefficient loops and algorithmic issues
- Review caching opportunities for expensive operations

### Code quality

- Functions should be focused and appropriately sized
- Use clear, descriptive naming conventions
- Ensure proper error handling throughout

### Review style

- Be specific and actionable in feedback
- Always explain the "why" behind recommendations
- Acknowledge good patterns when you see them
- Ask clarifying questions when code intent is unclear

Always prioritize security vulnerabilities and performance issues that could impact users.

Always suggest changes to improve readability.
