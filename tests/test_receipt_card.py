from io import BytesIO
from pathlib import Path

from PIL import Image

import base64
import importlib.util
import unittest


SPEC = importlib.util.spec_from_file_location("receipt_card", Path(__file__).resolve().parents[1] / "receipt_card.py")
card = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(card)

SAMPLE = '''# 执行结果

已完成 **DSH 桥接** 的回执排版。

- 支持中文、列表和 `inline code`
- 长内容自动分页

| 项目 | 状态 |
| --- | --- |
| 图片卡片 | 已启用 |
| QQ 投递 | 等待实测 |

```python
print("你好，HAKFOO")
```

[项目说明](https://example.com/docs)
'''


class ReceiptCardTests(unittest.TestCase):
    def test_markdown_renders_to_bounded_png_pages(self):
        pages = card.render_cards(SAMPLE)
        self.assertTrue(pages)
        for page in pages:
            with Image.open(BytesIO(base64.b64decode(page))) as image:
                self.assertEqual(image.format, "PNG")
                self.assertEqual(image.width, card.WIDTH)
                self.assertLessEqual(image.height, 154 + card.PAGE_LINES * card.LINE_HEIGHT)

    def test_long_response_is_paginated(self):
        pages = card.render_cards("\n\n".join(f"第 {index} 行：完整保留。" for index in range(100)))
        self.assertGreater(len(pages), 1)


if __name__ == "__main__":
    unittest.main()
