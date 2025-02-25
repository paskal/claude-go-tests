# Claude Go Tests Template

This repository serves as a template for writing robust Go tests using Claude AI. By forking this repository and connecting it to a Claude Project, you'll provide Claude with clear examples and context so it can generate tests that match your preferred style and best practices.

## Quick Start Guide

1. **[Fork](https://github.com/paskal/claude-go-tests/fork)** claude-go-tests repository repository using the link.

    ![Fork Button Screenshot](images/github-fork-creation.png)

2. **Create a Claude Project** (requires Claude Pro/Team subscription)
    - Navigate to [Claude](https://claude.ai) and sign in.
    - Open the **Projects** section from the menu on the left.
    - Click **[Create Project](https://claude.ai/projects/create)** at the top right and name it `go-tests`.
    - In the prompt field, enter:
      ```plaintext
      Project for writing tests for Go code.
      ```
    - Click **Create Project**.

        ![New Project Screenshot](images/claude-project-creation.png)

3. **Connect Your Fork**
    - Inside your Claude project, click **Add Content** → **GitHub**.

        ![Adding GitHub Context Screenshot](images/claude-project-add-context.png)

    - If you haven't authorized Claude for GitHub yet, click **Authorize**.

        ![GitHub Authorization Screenshot](images/claude-github-permissions.png)

    - Select your forked `claude-go-tests` repository from the list.
    - **Select only the `clauge-go-tests` directory** — this folder contains the instructions and examples that Claude requires.

        [Repository Selection Screenshot](images/claude-add-context.png)

    - Click **Add Selected Files**.

        [Context Selection Screenshot](images/claude-project-knowledge.png)

4. **Start Writing Tests**
    - Drag and drop your Go file into the chat on the left.
    - Use the following prompt as a starting point:

        ```plaintext
        Please write tests for this Go code following the instructions in your context.
        ```

        [Chat Example Screenshot](images/claude-new-prompt.png)

## Repository Structure

```plaintext
claude-go-tests/
├── README.md                  # This file
├── images/                    # Screenshots and documentation images
└── clauge-go-tests/            # Instructions and examples for Claude
     ├── INSTRUCTIONS.md       # Detailed instructions for Claude
     ├── package/              # Example Go code files grouped by package
        ├── *.go                  # Sample Go code files
        ├── *_test.go             # Example test implementations
```

## What's Included

- **Test Patterns**: Examples of common Go testing strategies such as table-driven tests, mocks, and fixtures.
- **Style Guide**: Conventions for consistent test naming and structure.
- **Edge Cases**: Samples addressing error conditions and boundary scenarios.
- **Best Practices**: Guidelines to help ensure your tests are maintainable and effective.

## Why Use This Template?

1. **Consistent Testing**: Claude learns your preferred testing style from the examples in `clauge-go-tests`.
2. **Time Efficiency**: Quickly receive well-structured test suggestions that you can review and adapt.
3. **Best Practices**: Leverage established Go testing patterns and conventions.
4. **Learning Resource**: Ideal for teams looking to standardize and improve their testing approach.

## Contributing

Contributions are welcome! If you have suggestions for new test patterns or improvements to the examples, please submit a pull request.

Feel free to add new directories for other usecases than the ones already present.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

Existing examples in the `clauge-go-tests` directory are sourced from MIT projects by [@umputun](https://github.com/umputun) and [@paskal](https://github.com/paskal).

---

## Official Documentation

For more details about Claude's GitHub integration, please see the [official documentation](https://support.anthropic.com/en/articles/10167454-using-the-github-integration).

---

Happy testing with Claude!