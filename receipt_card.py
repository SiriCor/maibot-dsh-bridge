"""Render Markdown receipts locally, without loading remote images or executing HTML."""

from io import BytesIO, StringIO
from pathlib import Path
from typing import List

from PIL import Image, ImageDraw, ImageFont
from rich.console import Console
from rich.markdown import Markdown
from rich.theme import Theme

import base64
import os


WIDTH = 960
PAGE_LINES = 36
LINE_HEIGHT = 38
FONT_SIZE = 25
BACKGROUND = "#101c2a"
FOREGROUND = "#dfebf4"


def _font(bold: bool = False) -> ImageFont.FreeTypeFont:
    """Use a CJK font explicitly; missing fonts are a deployment error."""
    fonts = Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts"
    candidates = [
        fonts / ("msyhbd.ttc" if bold else "msyh.ttc"),
        Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc" if bold else
             "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
    ]
    for path in candidates:
        if path.is_file():
            return ImageFont.truetype(str(path), FONT_SIZE)
    raise FileNotFoundError("DSH 回执卡片需要微软雅黑或 Noto Sans CJK 字体")


def render_cards(markdown: str) -> List[str]:
    """Return base64 PNG pages. Rich parses headings, lists, tables and code safely.

    Render one page at a time to bound image memory. Explicit line pagination
    preserves all content and makes retry progress independent of message limits.
    """
    regular, bold = _font(), _font(True)
    console = Console(file=StringIO(), width=64, color_system="truecolor", theme=Theme({
        "markdown.h1": "bold #7ae0d4", "markdown.h2": "bold #7ae0d4",
        "markdown.h3": "bold #7ae0d4", "markdown.link": "#83c8ff",
        "markdown.link_url": "#83c8ff", "markdown.code": "#7ae0d4",
    }))
    lines = console.render_lines(
        Markdown(markdown, hyperlinks=False, code_theme="monokai", justify="left"), pad=False,
    )
    pages = []
    total = max(1, (len(lines) + PAGE_LINES - 1) // PAGE_LINES)
    for number in range(total):
        page_lines = lines[number * PAGE_LINES:(number + 1) * PAGE_LINES]
        height = 154 + max(1, len(page_lines)) * LINE_HEIGHT
        canvas = Image.new("RGB", (WIDTH, height), "#09121e")
        draw = ImageDraw.Draw(canvas)
        draw.rounded_rectangle((16, 16, WIDTH - 17, height - 17), radius=22, fill=BACKGROUND)
        draw.rounded_rectangle((40, 42, 46, 72), radius=3, fill="#7ae0d4")
        draw.text((62, 39), "泰缇斯 · 回复", font=bold, fill="#7ae0d4")
        draw.text((WIDTH - 130, 42), f"{number + 1} / {total}", font=regular, fill="#91a9bb")
        draw.line((40, 88, WIDTH - 40, 88), fill="#2b3c4e", width=1)
        for row, segments in enumerate(page_lines):
            # Measure actual CJK glyph widths instead of assuming terminal cells
            # match pixels. Wide lines are resized as a whole, never cropped.
            lengths = [regular.getlength(segment.text) if not (segment.style and segment.style.bold)
                       else bold.getlength(segment.text) for segment in segments]
            line_width = max(1, int(sum(lengths)) + 2)
            strip = Image.new("RGB", (line_width, LINE_HEIGHT), BACKGROUND)
            painter = ImageDraw.Draw(strip)
            x = 0.0
            for segment, length in zip(segments, lengths):
                style = segment.style
                color = style.color.get_truecolor().hex if style and style.color else FOREGROUND
                if style and style.bgcolor:
                    painter.rectangle((x, 0, x + length, LINE_HEIGHT), fill=style.bgcolor.get_truecolor().hex)
                font = bold if style and style.bold else regular
                painter.text((x, 1), segment.text, font=font, fill=color)
                if style and style.underline:
                    painter.line((x, FONT_SIZE + 5, x + length, FONT_SIZE + 5), fill=color)
                x += length
            if strip.width > WIDTH - 88:
                strip = strip.resize((WIDTH - 88, LINE_HEIGHT), Image.Resampling.LANCZOS)
            canvas.paste(strip, (44, 104 + row * LINE_HEIGHT))
        output = BytesIO()
        canvas.save(output, format="PNG")
        pages.append(base64.b64encode(output.getvalue()).decode("ascii"))
    return pages
