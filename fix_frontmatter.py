#!/usr/bin/env python3
"""Fix the frontmatter in PipeLangNew.py to use correct OpenWebUI format"""

with open('PipeLangNew.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find where the YAML frontmatter ends
end_idx = 0
for i, line in enumerate(lines):
    if i > 0 and line.startswith('---') and lines[0].startswith('---'):
        end_idx = i + 1
        break

if end_idx > 0:
    # Remove lines 0 to end_idx-1 (the YAML frontmatter)
    remaining_lines = lines[end_idx:]
    
    # Add the correct frontmatter
    new_content = """# -*- coding: utf-8 -*-
\"\"\"
title: OpenAgent Clone - LangGraph Pipeline
author: Marcelo
version: 3.0.0
requirements: langgraph>=0.3.5,langchain>=0.2.0,langchain-openai>=0.1.0
license: MIT
description: LangGraph-based research orchestration pipeline for company profile analysis
\"\"\"

"""
    
    # Write the file back
    with open('PipeLangNew.py', 'w', encoding='utf-8') as f:
        f.write(new_content)
        f.writelines(remaining_lines)
    
    print(f'Fixed frontmatter: removed YAML (lines 0-{end_idx-1})')
    print(f'Added correct OpenWebUI format')
else:
    print('No YAML frontmatter found')
