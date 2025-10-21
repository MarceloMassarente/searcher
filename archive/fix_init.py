#!/usr/bin/env python3
"""Fix Pipe.__init__ to load environment variables"""

import os

with open('PipeLangNew.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the exact pattern to replace
old_pattern = '''    def __init__(self):
        self.valves = self.Valves()
        self._last_contract = None'''

new_pattern = '''    def __init__(self):
        self.valves = self.Valves(
            OPENAI_API_KEY=os.getenv("OPENAI_API_KEY", ""),
            OPENAI_BASE_URL=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
            LLM_MODEL=os.getenv("LLM_MODEL", "gpt-4o"),
        )
        self._last_contract = None'''

if old_pattern in content:
    content = content.replace(old_pattern, new_pattern)
    with open('PipeLangNew.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print('✅ Pipe.__init__ updated successfully!')
else:
    print('❌ Pattern not found in file')
    print('Looking for Valves initialization...')
    if 'self.valves = self.Valves()' in content:
        print('Found: self.valves = self.Valves()')
    if 'def __init__(self):' in content:
        print('Found: def __init__(self):')
