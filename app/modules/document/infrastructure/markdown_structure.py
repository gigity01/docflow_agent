"""清洗和切块共用的 Markdown 围栏与章节识别。"""

import re
from collections.abc import Sequence

HEADING_PATTERN = re.compile(r"^[ \t]{0,3}(#{1,6})[ \t]+(.+?)\s*$")
FENCE_PATTERN = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")


class CodeFence:
    def __init__(self) -> None:
        self.marker = ""
        self.length = 0

    def contains(self, line: str) -> bool:
        """返回当前行是否属于代码围栏，包括开闭标记。"""
        match = FENCE_PATTERN.match(line)
        if self.marker:
            if (match and match[1][0] == self.marker
                    and len(match[1]) >= self.length and not match[2].strip()):
                self.marker = ""
                self.length = 0
            return True
        if match and not (match[1][0] == "`" and "`" in match[2]):
            self.marker, self.length = match[1][0], len(match[1])
            return True
        return False


def extract_sections(lines: Sequence[str]) -> list[dict]:
    sections: list[dict] = []
    stack: list[tuple[int, str]] = []
    current = None
    fence = CodeFence()
    for number, line in enumerate(lines, 1):
        match = None if fence.contains(line) else HEADING_PATTERN.match(line)
        if match:
            if current is not None:
                current["end_line"] = number - 1
                sections.append(current)
            level, title = len(match[1]), match[2].strip()
            while stack and stack[-1][0] >= level:
                stack.pop()
            stack.append((level, title))
            current = {"level": level, "title": title,
                       "section_path": [name for _, name in stack],
                       "heading_line": number, "start_line": number, "end_line": number}
        elif current is None and line.strip():
            current = {"level": None, "title": None, "section_path": [],
                       "heading_line": None, "start_line": number, "end_line": number}
    if current is not None:
        current["end_line"] = len(lines)
        sections.append(current)
    return sections
