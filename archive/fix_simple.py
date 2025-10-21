with open('PipeLangNew.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace lines 6933-6934 - add 4 spaces of indentation
lines = content.split('\n')

# Find the line with "if self.valves.DEBUG_LOGGING:" around line 6932
for i in range(len(lines)):
    if i > 6925 and i < 6940 and 'if self.valves.DEBUG_LOGGING:' in lines[i]:
        # Check next line - if it starts with "                await" (16 spaces), change to 20 spaces
        if i+1 < len(lines) and lines[i+1].startswith('                await _safe_emit') and 'Auto-ajuste timeout' in lines[i+1]:
            lines[i+1] = '                    ' + lines[i+1].lstrip()
            # Also fix next line (yield)
            if i+2 < len(lines) and lines[i+2].startswith('                yield f"**[DEBUG]** Auto-ajuste'):
                lines[i+2] = '                    ' + lines[i+2].lstrip()
            print(f'✅ Fixed indentation at lines {i+2}-{i+3}')
            break

content = '\n'.join(lines)
with open('PipeLangNew.py', 'w', encoding='utf-8') as f:
    f.write(content)
