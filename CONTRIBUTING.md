# Contributing guidelines

## Setup your environment

1. Install [uv](https://docs.astral.sh/uv/getting-started/installation/).

2. [Fork](https://github.com/mdverse/mdws/fork) this repository.

3. Clone your fork on your machine:

    ```sh
    git clone https://github.com/<your-github-username>/mdws.git
    ```

4. Get into the newly created directory:

    ```sh
    cd mdws
    ```

5. Install dependencies:

    ```sh
    uv sync --dev
    ```

6. Install pre-commit hook:

    ```sh
    uv run prek install
    ```

7. Create a new branch:

    ```sh
    git switch -c <your-branch-name>
    ```

8. Make changes and commit:

    ```sh
    git add
    git commit -m "<your-commit-message>"
    git push origin "<your-branch-name>"
    ```

9. Create a [pull request](https://github.com/mdverse/mdws/compare).

    Click compare across forks if you don't see your branch.

## Use Conventional commits

For commit messages, please use Conventional Commits.

See:

- [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/): reference guide
- Cheatsheet of [Sample Conventional Commit Verbs](https://gkarthiks.github.io/quick-commands-cheat-sheet/conventional-commit-verbs)
- [Conventional Commit](https://blog.stephane-robert.info/docs/developper/conventional-commits/) (FR)

Examples of valid commit messages:

- `feat: Valide metadata with model`
- `fix(http): Resolve multiple errors on HTTP queries`
- `fix: Verify directory exists before writing in (#45)`
- `docs: Update installation instructions`
- `style: Format code according with ruff`
- `refactor: Move CLI argument management into separate module`
- `test: Add unit tests for the new scraper`
- `chore: Update dependencies to latest versions`
