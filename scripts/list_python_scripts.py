#!/usr/bin/env python3
import os
import ast
import pathlib

def extract_description(filepath):
    """Extract docstring or first comments from a python file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Try to parse AST to get module docstring
        try:
            tree = ast.parse(content)
            docstring = ast.get_docstring(tree)
            if docstring:
                # Return first line of docstring
                return docstring.split('\n')[0].strip()
        except SyntaxError:
            pass
            
        # Fallback to first few lines of comments
        lines = content.split('\n')
        comment_lines = []
        for line in lines[:10]:
            trimmed = line.strip()
            if trimmed.startswith('#!'):
                continue
            if trimmed.startswith('#'):
                comment_lines.append(trimmed.lstrip('#').strip())
            elif trimmed:
                break
        if comment_lines:
            return ' '.join(comment_lines)[:120]
    except Exception as e:
        return f"Error reading file: {str(e)}"
    return "No description available."

def list_scripts(root_dir):
    root = pathlib.Path(root_dir)
    ignored_dirs = {'.venv', '.git', '.aws-sam', 'target', '__pycache__'}
    
    scripts_by_folder = {}
    
    for path in root.rglob('*.py'):
        # Check if any parent is in ignored_dirs
        parts = path.relative_to(root).parts
        if any(ignored in parts for ignored in ignored_dirs):
            continue
            
        # Get folder path relative to root
        folder = str(path.parent.relative_to(root))
        if folder == '.':
            folder = 'Root'
            
        description = extract_description(path)
        size_kb = path.stat().st_size / 1024
        
        scripts_by_folder.setdefault(folder, []).append({
            'name': path.name,
            'path': str(path.relative_to(root)),
            'size': f"{size_kb:.2f} KB",
            'desc': description
        })
        
    # Sort folders
    for folder in sorted(scripts_by_folder.keys()):
        print(f"## Folder: `{folder}`")
        print("| Script | Path | Size | Description |")
        print("| --- | --- | --- | --- |")
        # Sort scripts by name
        for script in sorted(scripts_by_folder[folder], key=lambda x: x['name']):
            # Provide exact clickable file:// link
            abs_path = root / script['path']
            print(f"| `{script['name']}` | [{script['path']}](file://{abs_path}) | {script['size']} | {script['desc']} |")
        print()

if __name__ == '__main__':
    # Determine the directory where this script sits, relative to the root
    script_path = pathlib.Path(__file__).resolve()
    engine_root = script_path.parent.parent
    list_scripts(engine_root)
