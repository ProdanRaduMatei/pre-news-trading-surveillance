from __future__ import annotations

from html import escape
import re


def render_markdown_page(
    *,
    title: str,
    eyebrow: str,
    intro: str,
    markdown_text: str,
    actions: list[tuple[str, str]],
    side_panel_title: str,
    side_panel_body: str,
    extra_html: str = "",
) -> str:
    actions_html = "".join(
        f'<a class="button {"button-primary" if index == 0 else "button-secondary"}" href="{escape(href, quote=True)}">{escape(label)}</a>'
        for index, (href, label) in enumerate(actions)
    )
    prose_html = _markdown_to_html(markdown_text)
    return f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{escape(title)} | Pre-News Trading Surveillance</title>
    <meta name="description" content="{escape(intro, quote=True)}" />
    <link rel="stylesheet" href="/static/styles.css" />
  </head>
  <body>
    <div class="backdrop backdrop-aurora"></div>
    <div class="backdrop backdrop-grid"></div>
    <div class="page-shell">
      <header class="hero hero-doc">
        <div class="hero-copy reveal">
          <p class="eyebrow">{escape(eyebrow)}</p>
          <h1>{escape(title)}</h1>
          <p class="hero-text">{escape(intro)}</p>
          <div class="hero-actions">{actions_html}</div>
        </div>
        <section class="hero-panel reveal reveal-delay-1">
          <div class="hero-panel-header">
            <span class="panel-kicker">Public Research Guardrails</span>
          </div>
          <div class="doc-side-panel">
            <h2>{escape(side_panel_title)}</h2>
            <p>{escape(side_panel_body)}</p>
          </div>
        </section>
      </header>
      <main class="dashboard">
        {extra_html}
        <article class="panel reveal">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">{escape(eyebrow)}</p>
              <h2>{escape(title)}</h2>
            </div>
          </div>
          <div class="prose">{prose_html}</div>
        </article>
      </main>
    </div>
  </body>
</html>
"""


def _markdown_to_html(markdown_text: str) -> str:
    lines = markdown_text.splitlines()
    blocks: list[str] = []
    paragraph: list[str] = []
    bullet_items: list[str] = []
    number_items: list[str] = []
    in_code = False
    code_lines: list[str] = []

    def flush_paragraph() -> None:
        nonlocal paragraph
        if paragraph:
            blocks.append(f"<p>{_format_inline(' '.join(paragraph))}</p>")
            paragraph = []

    def flush_bullets() -> None:
        nonlocal bullet_items
        if bullet_items:
            blocks.append("<ul>" + "".join(f"<li>{item}</li>" for item in bullet_items) + "</ul>")
            bullet_items = []

    def flush_numbers() -> None:
        nonlocal number_items
        if number_items:
            blocks.append("<ol>" + "".join(f"<li>{item}</li>" for item in number_items) + "</ol>")
            number_items = []

    for raw_line in lines:
        line = raw_line.rstrip()
        stripped = line.strip()

        if stripped.startswith("```"):
            flush_paragraph()
            flush_bullets()
            flush_numbers()
            if in_code:
                blocks.append("<pre><code>" + escape("\n".join(code_lines)) + "</code></pre>")
                code_lines = []
                in_code = False
            else:
                in_code = True
            continue

        if in_code:
            code_lines.append(raw_line)
            continue

        if not stripped:
            flush_paragraph()
            flush_bullets()
            flush_numbers()
            continue

        if stripped.startswith("# "):
            flush_paragraph()
            flush_bullets()
            flush_numbers()
            blocks.append(f"<h2>{_format_inline(stripped[2:])}</h2>")
            continue

        if stripped.startswith("## "):
            flush_paragraph()
            flush_bullets()
            flush_numbers()
            blocks.append(f"<h3>{_format_inline(stripped[3:])}</h3>")
            continue

        if stripped.startswith("### "):
            flush_paragraph()
            flush_bullets()
            flush_numbers()
            blocks.append(f"<h4>{_format_inline(stripped[4:])}</h4>")
            continue

        if stripped.startswith("- "):
            flush_paragraph()
            flush_numbers()
            bullet_items.append(_format_inline(stripped[2:]))
            continue

        numbered = re.match(r"^(\d+)\.\s+(.*)$", stripped)
        if numbered:
            flush_paragraph()
            flush_bullets()
            number_items.append(_format_inline(numbered.group(2)))
            continue

        paragraph.append(stripped)

    flush_paragraph()
    flush_bullets()
    flush_numbers()
    if in_code:
        blocks.append("<pre><code>" + escape("\n".join(code_lines)) + "</code></pre>")

    return "\n".join(blocks)


def _format_inline(text: str) -> str:
    pattern = re.compile(r"\[([^\]]+)\]\(([^)]+)\)|`([^`]+)`")
    parts: list[str] = []
    cursor = 0
    for match in pattern.finditer(text):
        parts.append(escape(text[cursor : match.start()]))
        if match.group(1) is not None and match.group(2) is not None:
            parts.append(
                f'<a href="{escape(match.group(2), quote=True)}">{escape(match.group(1))}</a>'
            )
        elif match.group(3) is not None:
            parts.append(f"<code>{escape(match.group(3))}</code>")
        cursor = match.end()
    parts.append(escape(text[cursor:]))
    return "".join(parts)
