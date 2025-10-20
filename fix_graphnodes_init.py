#!/usr/bin/env python3
"""Initialize LLM components in GraphNodes.__init__"""

with open('PipeLangNew.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_code = '''        # Instanciar LLM components
        self.analyst = None
        self.judge = None
        self.deduplicator = None'''

new_code = '''        # Initialize LLM components
        self.analyst = AnalystLLM(self.valves)
        self.judge = JudgeLLM(self.valves)
        self.deduplicator = Deduplicator(self.valves)'''

if old_code in content:
    content = content.replace(old_code, new_code)
    with open('PipeLangNew.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("✅ Fixed GraphNodes LLM component initialization")
else:
    print("❌ Could not find code pattern")
