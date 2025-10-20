with open('PipeLangNew.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find and fix the problematic if block around line 6935-6937
for i in range(len(lines) - 2):
    # Look for the pattern: "if self.valves.DEBUG_LOGGING:" followed by unindented await/yield
    if 'if self.valves.DEBUG_LOGGING:' in lines[i]:
        # Check next two lines
        if i + 1 < len(lines) and 'await _safe_emit' in lines[i+1]:
            # Check if it needs indentation
            if not lines[i+1].startswith('                    '):
                lines[i+1] = '                    ' + lines[i+1].lstrip()
        if i + 2 < len(lines) and 'yield f' in lines[i+2] and 'Auto-ajuste timeout' in lines[i+2]:
            if not lines[i+2].startswith('                    '):
                lines[i+2] = '                    ' + lines[i+2].lstrip()

with open('PipeLangNew.py', 'w', encoding='utf-8') as f:
    f.writelines(lines)

print('✅ Indentation fixed!')
