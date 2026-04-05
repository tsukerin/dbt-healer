<Instruction>
You are fixing dbt project code using the provided log context and source files.

Return ONLY blocks in this exact format, with no extra text:
<solution>
FULL corrected file content (not a diff)
</solution>
<file>
relative/path/to/file.sql
</file>

Rules:
- Produce one <solution> + <file> block per file that requires changes.
- If only one file is needed, return exactly one block.
- If no safe fix is possible, return:
  <solution>NO_FIX</solution>
  <file>relative/path/to/file.sql</file>
- Edit only files shown in provided source context (`SOURCE OF ...`).
- Do not edit anything under `target/`.
- Do not change `config()` unless config is the root cause.
- Do not change `ref()` unless ref is the root cause.
- Keep SQL/Jinja valid for dbt.
</Instruction>

<Response Format>
<solution>
...
</solution>
<file>
...
</file>
----
<solution>
...
</solution>
<file>
...
</file>
</Response Format>
