# Data Mining Graph Algorithms

This collaborative project implements and evaluates various graph mining algorithms using the MapReduce framework.

---

## 🗂 Project Structure & Guidelines

### 📁 Algorithm Organization
Each contributor is responsible for specific algorithms. To keep the project organized:

- **Create a directory for your assigned algorithm**, named using kebab-case.  
  Example: `minimum-spanning-tree`

- Inside your algorithm directory, include:
  - A `src/` folder containing your source code.
  - A `README.md` explaining:
    - How to install dependencies (if any)
    - How to run your code
    - What results to expect (briefly)
  - A `datasets/` folder (if applicable):
    - Only include small datasets that are safe to push to GitHub.
    - If using external data, explain how to fetch it (via API or SDK) in your `README.md`.

---

## 🔀 Branching & Collaboration Rules

### 🌿 Creating Your Branch
- **Do not commit directly to the `main` branch**.
- When starting your work, create a new branch using the following naming convention:

  ```firstname-algorithm-branch-#```

  - `firstname`: Your first name
  - `algorithm`: Name of the algorithm you're working on
  - `#`: The branch number (increment this if you create multiple branches for the same algorithm)

  **Example:**  
  `aime-page-rank-branch-1`

### 🔧 Contributing to Someone Else’s Algorithm
- If you're helping on another algorithm:
  1. Create your own branch using the same format (with your name).
  2. Set up the algorithm locally by following the original contributor's `README.md`.
  3. **Communicate** with the original contributor before making changes—they can provide guidance or context.

---

## ✅ Pull Requests
- Once your implementation is in a stable and functional state:
  - Submit a **Pull Request (PR)** to merge your branch into `main`.
  - Clearly describe:
    - What you implemented
    - Any known issues or limitations
    - If applicable, what kind of review you want (e.g., code style, algorithm accuracy, etc.)

---

## 💡 Additional Tips
- **Commit often and write clear commit messages.** This helps in tracking changes and understanding your development process.
- **Use `.gitignore`** to avoid committing unnecessary files (e.g., compiled binaries, `.DS_Store`, IDE configs).
- Keep your code modular and readable—other team members might need to understand or modify it later.
- Regularly **pull from `main`** to keep your branch up to date and avoid merge conflicts.
- To ignore all `.idea/` directories and `.gitignore` files located **anywhere in the project**, including in subdirectories, add the following lines to your root `.gitignore`:

    ```gitignore
    **/.idea/
    **/.gitignore
    ```

  You can be a bit more cautious with:  

  ```gitignore
  **/.idea
  **/.idea/
  **/.gitignore
  ```