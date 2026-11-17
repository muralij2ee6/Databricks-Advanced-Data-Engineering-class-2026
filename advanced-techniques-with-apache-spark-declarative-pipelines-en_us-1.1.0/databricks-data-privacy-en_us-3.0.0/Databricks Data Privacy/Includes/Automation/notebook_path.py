# Databricks notebook source
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Course-specific inputs

# COMMAND ----------

# Change the course name as per your course name
COURSE_NAME = "Databricks Data Privacy"
RUN_QA_CHECKER = True
QA_TASK_RELATIVE_PATH = "./qa_content_checker"
QA_TASK_NAME = "QA Content Checker — Grammar, Deprecated, UI Steps"

# Compute: all tasks (course notebooks + QA checker) run on Serverless, environment version 6
SERVERLESS_ENV_VERSION = "6"

# Tester emails — notified on job failure and success. Add as many as you like with comma separated emails
TESTER_EMAILS = [
    "nitesh.ojha@databricks.com"
]

# Add your lab notebook paths relative from the CourseRunner folder as per your course
LAB_NOTEBOOKS = [
    "../../05 Lab - Discover, Tag and Protect PII",
    "../../10 Lab - Fulfil and Verify an Erasure Request",
]

# Define the course notebook DAG here so this can be used for job scheduling as per your course
# Lecture notebooks (02, 09) and the Course Overview (00) have no runnable code
# and no Classroom-Setup, so they are excluded from the job DAG.
# Relative paths are resolved from this Automation folder (Includes/Automation).
COURSE_TASKS = [
    {
        "task_key": "01 Demo - REQUIRED - Project Overview and Setup",
        "name": "01 Demo - REQUIRED - Project Overview and Setup",
        "relative_path": "../../01 Demo - REQUIRED - Project Overview and Setup",
        "depends_on": [],
        "env_key": "course_01",
        "use_classic": False,
    },
    {
        "task_key": "03 Demo - Discover and Classify Sensitive Data",
        "name": "03 Demo - Discover and Classify Sensitive Data",
        "relative_path": "../../03 Demo - Discover and Classify Sensitive Data",
        "depends_on": [
            {"task_key": "01 Demo - REQUIRED - Project Overview and Setup"},
        ],
        "env_key": "course_03",
        "use_classic": False,
    },
    {
        "task_key": "04 Demo - ABAC and Manual Masks",
        "name": "04 Demo - ABAC and Manual Masks",
        "relative_path": "../../04 Demo - ABAC and Manual Masks",
        "depends_on": [
            {"task_key": "03 Demo - Discover and Classify Sensitive Data"},
        ],
        "env_key": "course_04",
        "use_classic": False,
    },
    {
        "task_key": "05 Lab - Discover, Tag and Protect PII",
        "name": "05 Lab - Discover, Tag and Protect PII",
        "relative_path": "../../05 Lab - Discover, Tag and Protect PII",
        "depends_on": [
            {"task_key": "04 Demo - ABAC and Manual Masks"},
        ],
        "env_key": "course_05_lab",
        "use_classic": False,
    },
    {
        "task_key": "06 Demo - De-identification and Key Management",
        "name": "06 Demo - De-identification and Key Management",
        "relative_path": "../../06 Demo - De-identification and Key Management",
        "depends_on": [
            {"task_key": "05 Lab - Discover, Tag and Protect PII"},
        ],
        "env_key": "course_06",
        "use_classic": False,
    },
    {
        "task_key": "07 Demo - Right to Erasure - Delete, Propagate, Purge",
        "name": "07 Demo - Right to Erasure - Delete, Propagate, Purge",
        "relative_path": "../../07 Demo - Right to Erasure - Delete, Propagate, Purge",
        "depends_on": [
            {"task_key": "06 Demo - De-identification and Key Management"},
        ],
        "env_key": "course_07",
        "use_classic": False,
    },
    {
        "task_key": "08 Demo - Privacy Evidence with System Tables",
        "name": "08 Demo - Privacy Evidence with System Tables",
        "relative_path": "../../08 Demo - Privacy Evidence with System Tables",
        "depends_on": [
            {"task_key": "07 Demo - Right to Erasure - Delete, Propagate, Purge"},
        ],
        "env_key": "course_08",
        "use_classic": False,
    },
    {
        "task_key": "10 Lab - Fulfil and Verify an Erasure Request",
        "name": "10 Lab - Fulfil and Verify an Erasure Request",
        "relative_path": "../../10 Lab - Fulfil and Verify an Erasure Request",
        "depends_on": [
            {"task_key": "08 Demo - Privacy Evidence with System Tables"},
        ],
        "env_key": "course_10_lab",
        "use_classic": False,
    },
]

# COMMAND ----------

# ── Auto-Fill Lab Notebooks ──────────────────────────────────────────────────
# This script finds unresolved lab placeholders in lab notebooks and replaces
# them with solution code extracted from the nearby solution blocks.
#
# HOW IT WORKS:
# 1. Exports each lab notebook as .ipynb JSON
# 2. Scans code cells for unresolved markers such as <FILL_IN>, <FILL-IN>,
#    <ToDo>, and TODO-only scaffolds
# 3. Looks ahead in subsequent markdown cells for <code>...</code> blocks
# 4. Extracts the solution and replaces the original cell source
# 5. Second pass (Data Privacy lab format): finds "Reveal the solution"
#    <details><pre>...</pre> markdown cells, walks back to the nearest
#    comment-only YOUR TASK code cell (e.g. %sql + -- instructions), skipping
#    helper/check cells like verify_tags(), and fills it with the <pre> code.
# 6. Re-imports the patched notebook
#
# Safe to re-run: it only patches cells that still contain unresolved markers.
# ──────────────────────────────────────────────────────────────────────────────

import base64
import json
import os
import re
import html as html_lib
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language

w = WorkspaceClient()

# Derive course_root from this notebook's own path
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
course_root = "/".join(_nb_path.split("/")[:-1])

# LAB_NOTEBOOKS is configured in the course-specific inputs cell above.
# Resolve to absolute workspace paths
lab_abs_paths = [os.path.normpath(f"{course_root}/{p}") for p in LAB_NOTEBOOKS]

PLACEHOLDER_PATTERNS = [
    re.compile(r'<\s*fill[-_ ]?in\s*>', re.IGNORECASE),
    re.compile(r'--\s*fill[-_ ]?in\s*--', re.IGNORECASE),
    re.compile(r'<\s*todo\s*>', re.IGNORECASE),
    re.compile(r'(?mi)^\s*#+\s*todo\b'),
    re.compile(r'(?mi)^\s*#+.*\bTODO\b'),
    # SQL lab pattern: -- TODO comment at the start of a cell (used in Databricks Academy SQL labs)
    re.compile(r'(?mi)^\s*--\s*TODO\b'),
    # Bare FILL_IN used as a Python identifier (e.g., payments_schema = FILL_IN)
    re.compile(r'\bFILL_IN\b'),
    # Generic uppercase angle-bracket placeholders (catches <COMPLETAR>, <PREENCHA>, <AUSFÜLLEN>, etc.)
    re.compile(r'<[A-Z][A-Z_]{2,}>'),
]

# Multilingual '-- ANSWER' header pattern (reused across first-pass detection, extraction, and stop condition)
ANSWER_RE = re.compile(r'\s*--\s*(?:ANSWER|답변?|정답|回答|解答|RESPOSTA|RÉPONSE|ANTWORT|RISPOSTA)\b', re.IGNORECASE)


def has_unresolved_placeholder(source: str) -> bool:
    """Return True when a code cell still contains a fill-in or TODO scaffold."""
    return any(pattern.search(source) for pattern in PLACEHOLDER_PATTERNS)


def extract_solution_from_source(source: str) -> str | None:
    """Extract code from a Databricks solution cell.

    Handles:
    - Fenced code blocks: ```python\\n...\\n``` (used in current labs)
    - HTML <div class="cb"> blocks with <br/> line separators (Databricks Academy format)
    - HTML <code> tags: <code>...</code> (legacy fallback)
    - Strips %md / %md-sandbox magic prefixes before parsing.
    """
    # Strip Databricks magic prefix (%md, %md-sandbox, etc.)
    clean = re.sub(r'^%md[a-zA-Z-]*[ \t]*\n', '', source.lstrip(), count=1)

    # 1. Fenced code block (most common in current labs)
    match = re.search(r'```(?:python|sql|scala|r|sh)?\s*\n(.*?)\n[ \t]*```', clean, re.DOTALL)
    if match:
        code = match.group(1)
        lines = code.split('\n')
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()
        return '\n'.join(lines) if lines else None

    # 2. Databricks Academy <div class="cb"> format (uses <br/> as line separators)
    match = re.search(r'<div\s+class=["\']cb["\'][^>]*>\s*\n?(.*?)\n?\s*</div>', clean, re.DOTALL)
    if match:
        raw = match.group(1)
        # Replace <br/> variants with newlines
        raw = re.sub(r'<br\s*/?>', '\n', raw)
        # Unescape HTML entities (&gt; &lt; &amp; etc.)
        code = html_lib.unescape(raw)
        lines = code.split('\n')
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()
        return '\n'.join(lines) if lines else None

    # 3. Databricks Academy <div class="code-block"> or <div class="code-block-dark"> format (Prism.js highlighted blocks)
    match = re.search(r'<div\s+class=["\']code-block(?:-dark)?["\'][^>]*>\s*\n?(.*?)\n?\s*</div>', clean, re.DOTALL)
    if match:
        raw = match.group(1)
        # Remove any nested HTML tags (e.g., <pre>, <code>, <br/>)
        raw = re.sub(r'<br\s*/?>', '\n', raw)
        raw = re.sub(r'<[^>]+>', '', raw)
        # Unescape HTML entities
        code = html_lib.unescape(raw)
        lines = code.split('\n')
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()
        return '\n'.join(lines) if lines else None

    # 4. HTML <code> tags (fallback for older lab format)
    match = re.search(r'<code[^>]*>\s*\n?(.*?)\n?\s*</code>', clean, re.DOTALL)
    if match:
        code = html_lib.unescape(match.group(1))
        lines = code.split('\n')
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()
        return '\n'.join(lines) if lines else None

    return None


def extract_answer_cell(source: str) -> str | None:
    """Extract solution from a SQL '-- ANSWER' code cell.

    Strips the leading '-- ANSWER' comment line and returns the remaining code.
    Used in Databricks Academy SQL lab notebooks where answers follow TODO cells
    directly as sibling code cells (rather than being embedded in markdown hint blocks).
    """
    lines = source.split('\n')
    result_lines = []
    skipping_answer_header = True
    for line in lines:
        if skipping_answer_header and ANSWER_RE.match(line):
            continue
        skipping_answer_header = False
        result_lines.append(line)
    # Strip leading/trailing blank lines
    while result_lines and not result_lines[0].strip():
        result_lines.pop(0)
    while result_lines and not result_lines[-1].strip():
        result_lines.pop()
    return '\n'.join(result_lines) if result_lines else None


def extract_skip_cell(source: str) -> str | None:
    """Extract solution from a '%skip' code cell.

    Strips the leading '%skip' line and returns the remaining code.
    Used in Databricks Academy labs where solution cells are marked with %skip magic.
    """
    lines = source.split('\n')
    result_lines = []
    skipping_header = True
    for line in lines:
        if skipping_header and re.match(r'\s*%skip\b', line, re.IGNORECASE):
            continue
        skipping_header = False
        result_lines.append(line)
    # Strip leading/trailing blank lines
    while result_lines and not result_lines[0].strip():
        result_lines.pop(0)
    while result_lines and not result_lines[-1].strip():
        result_lines.pop()
    return '\n'.join(result_lines) if result_lines else None


def is_solution_candidate(cell: dict) -> bool:
    """Return True if a cell is a potential answer/solution block.

    In Databricks ipynb exports, %md-sandbox cells are stored as code cells
    (not markdown), so we must check both cell_type=='markdown' and code cells
    that start with a %md magic prefix.
    """
    source = ''.join(cell.get("source", []))
    has_code_block = '```' in source or '<code>' in source or 'class="cb"' in source or "class='cb'" in source or 'code-block' in source
    is_markdown_type = cell.get("cell_type") == "markdown"
    is_md_magic = cell.get("cell_type") == "code" and bool(re.match(r'\s*%md', source))
    return (is_markdown_type or is_md_magic) and has_code_block


# ── Reveal-the-solution (<details><pre>) format helpers ──────────────────────
REVEAL_RE  = re.compile(r'<details', re.IGNORECASE)  # Structural match — works for any language
PRE_RE     = re.compile(r'<pre[^>]*>(.*?)</pre>', re.DOTALL)
SECTION_RE = re.compile(r'^\s*(%md[a-zA-Z-]*\s*)?#{1,3}\s', re.MULTILINE)  # ## / ### section headers
REVEAL_LOOKBACK = 6  # max cells to walk back from a solution cell to its task cell


def _cell_src(cell: dict) -> str:
    return ''.join(cell.get("source", []))


def _is_markdown_like(cell: dict) -> bool:
    s = _cell_src(cell)
    return cell.get("cell_type") == "markdown" or (
        cell.get("cell_type") == "code" and bool(re.match(r'\s*%md', s))
    )


def is_reveal_solution_cell(cell: dict) -> bool:
    """Markdown cell with a 'Reveal the solution' dropdown containing a <pre> code block."""
    s = _cell_src(cell)
    return _is_markdown_like(cell) and bool(REVEAL_RE.search(s)) and bool(PRE_RE.search(s))


def is_comment_only_task(cell: dict) -> bool:
    """Code cell holding only a magic line (e.g. %sql) plus comment instructions -- no runnable code."""
    if cell.get("cell_type") != "code":
        return False
    s = _cell_src(cell)
    if re.match(r'\s*%(md|run|pip|sh|fs)\b', s):
        return False
    lines = [l.strip() for l in s.split('\n') if l.strip()]
    body = [l for l in lines if not l.startswith('%')]
    return bool(body) and all(l.startswith(('--', '#')) for l in body)


def extract_pre_solution(source: str) -> str | None:
    """Return the code inside all <pre> blocks of a solution cell (HTML stripped and unescaped)."""
    blocks = []
    for raw in PRE_RE.findall(source):
        raw = re.sub(r'<br\s*/?>', '\n', raw)
        raw = re.sub(r'<[^>]+>', '', raw)
        code = html_lib.unescape(raw).strip('\n')
        if code.strip():
            blocks.append(code)
    return '\n\n'.join(blocks) or None


def fill_reveal_solutions(cells: list, notebook_path: str, stats: dict) -> None:
    """Fill comment-only YOUR TASK cells from the following 'Reveal the solution' <pre> block."""
    for i, cell in enumerate(cells):
        if not is_reveal_solution_cell(cell):
            continue

        # Walk back to the nearest comment-only task cell, skipping helper/check code cells.
        # Stop at the previous solution cell or a section header so we never cross tasks.
        target = None
        for k in range(i - 1, max(-1, i - 1 - REVEAL_LOOKBACK), -1):
            prev = cells[k]
            if is_reveal_solution_cell(prev) or (_is_markdown_like(prev) and SECTION_RE.search(_cell_src(prev))):
                break
            if is_comment_only_task(prev):
                target = k
                break
        if target is None:
            continue  # already filled on a previous run, or no task cell for this solution

        stats["blanks_found"] += 1
        solution = extract_pre_solution(_cell_src(cell))
        if solution:
            # Remove EXCEPT `account privileged` — group not provisioned in this workspace
            solution = re.sub(r"\s+EXCEPT\s+`account privileged`", "", solution)
        if not solution:
            stats["no_answer"] += 1
            print(f"  ⚠️  NO ANSWER FOUND in <pre> for task cell {target} in {notebook_path}")
            continue

        # Keep the task cell's magic (e.g. %sql) if the solution doesn't carry one.
        task_src = _cell_src(cells[target]).lstrip()
        magic = task_src.split('\n', 1)[0].strip() if task_src.startswith('%') else None
        if magic and not solution.lstrip().startswith('%'):
            solution = f"{magic}\n{solution}"

        cells[target]["source"] = [solution + '\n']
        stats["filled"] += 1
        print(f"  ✅ Filled task cell {target} from solution cell {i}")


def fill_notebook(notebook_path: str) -> dict:
    """Fill all unresolved placeholder cells in a single notebook. Returns stats."""
    stats = {"path": notebook_path, "blanks_found": 0, "filled": 0, "no_answer": 0}

    # Export notebook as ipynb
    export_resp = w.workspace.export(path=notebook_path, format=ExportFormat.JUPYTER)
    nb_json = json.loads(base64.b64decode(export_resp.content))
    cells = nb_json.get("cells", [])

    for i, cell in enumerate(cells):
        source = ''.join(cell.get("source", []))

        # Check if this cell still has unresolved fill-in / TODO markers
        if not has_unresolved_placeholder(source):
            continue

        # Skip markdown cells (we only fill code cells)
        if cell.get("cell_type") != "code":
            continue

        # Skip %md / %md-sandbox magic cells (they are code type in ipynb but render as markdown)
        if re.match(r'\s*%md', source):
            continue

        stats["blanks_found"] += 1

        # Look ahead up to 8 cells for a solution block.
        # %md-sandbox solution cells may appear as 'code' type in ipynb exports,
        # so is_solution_candidate() checks both cell_type and %md magic prefix.
        solution = None
        for j in range(i + 1, min(i + 9, len(cells))):
            next_cell = cells[j]
            next_source = ''.join(next_cell.get("source", []))
            # SQL lab pattern: -- ANSWER code cell immediately follows -- TODO cell
            if (next_cell.get("cell_type") == "code"
                    and ANSWER_RE.match(next_source)):
                solution = extract_answer_cell(next_source)
                if solution:
                    break
            # %skip pattern: solution cell with %skip magic prefix
            elif (next_cell.get("cell_type") == "code"
                    and re.match(r'\s*%skip\b', next_source, re.IGNORECASE)):
                solution = extract_skip_cell(next_source)
                if solution:
                    break
            elif is_solution_candidate(next_cell):
                solution = extract_solution_from_source(next_source)
                if solution:
                    break
            # Stop looking once we hit a real (non-magic, non-ANSWER) code cell
            elif (next_cell.get("cell_type") == "code"
                  and not re.match(r'\s*%', next_source)
                  and not ANSWER_RE.match(next_source)):
                break

        if solution:
            # Check if solution has %sql magic — if so, handle cell language
            if solution.strip().startswith('%sql'):
                # Remove %sql prefix, keep the SQL code
                solution_lines = solution.strip().split('\n')
                # Keep %sql as first line for the cell magic
                cell["source"] = [solution + '\n']
            else:
                cell["source"] = [solution + '\n']
            stats["filled"] += 1
        else:
            stats["no_answer"] += 1
            print(f"  ⚠️  NO ANSWER FOUND for cell {i} in {notebook_path}")

    # Second pass: comment-only YOUR TASK cells + 'Reveal the solution' <pre> blocks
    fill_reveal_solutions(cells, notebook_path, stats)

    # Re-import the patched notebook
    if stats["filled"] > 0:
        patched_content = base64.b64encode(json.dumps(nb_json).encode()).decode()
        # Auto-detect notebook language from ipynb kernel metadata
        _kernel_lang = nb_json.get("metadata", {}).get("kernelspec", {}).get("language", "python").lower()
        _lang_map = {"python": Language.PYTHON, "sql": Language.SQL, "scala": Language.SCALA, "r": Language.R}
        _nb_lang = _lang_map.get(_kernel_lang, Language.PYTHON)
        w.workspace.import_(
            path=notebook_path,
            content=patched_content,
            format=ImportFormat.JUPYTER,
            overwrite=True,
            language=_nb_lang,
        )

    return stats


# ── Run the filler ────────────────────────────────────────────────────────────
print("═" * 70)
print("AUTO-FILL LAB NOTEBOOKS")
print("═" * 70)

all_stats = []
for path in lab_abs_paths:
    print(f"\n▶ Processing: {path.split('/')[-1]}")
    try:
        s = fill_notebook(path)
        all_stats.append(s)
        print(f"   Blanks found: {s['blanks_found']}  |  Filled: {s['filled']}  |  No answer: {s['no_answer']}")
    except Exception as e:
        print(f"   ❌ ERROR: {e}")
        all_stats.append({"path": path, "blanks_found": 0, "filled": 0, "no_answer": 0, "error": str(e)})

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "═" * 70)
total_blanks = sum(s["blanks_found"] for s in all_stats)
total_filled = sum(s["filled"] for s in all_stats)
total_no_ans = sum(s["no_answer"] for s in all_stats)
print(f"TOTAL:  {total_blanks} blanks found  |  {total_filled} filled  |  {total_no_ans} blockers")
print("═" * 70)