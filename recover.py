import os, glob, re

logs_path = os.path.expanduser('~/.gemini/antigravity/brain/*/.system_generated/logs/overview.txt')
out_dir = os.path.expanduser('~/projects/metri/metri-engine/docs/architecture')
os.makedirs(out_dir, exist_ok=True)

file_contents = {}

for log_file in glob.glob(logs_path):
    print(f"Reading log {log_file}")
    try:
        with open(log_file, 'r', errors='ignore') as f:
            content = f.read()
    except Exception as e:
        continue
    
    parts = content.split('File Path: `file://')
    for part in parts[1:]:
        lines = part.split('\n')
        filepath = lines[0].strip().strip('`')
        if not filepath.endswith('.md'): continue
        
        body_start = -1
        for i, line in enumerate(lines[:20]):
            if 'The following code has been modified to include' in line:
                body_start = i + 1
                break
        
        if body_start != -1:
            parsed_lines = []
            for line in lines[body_start:]:
                match = re.match(r'^\d+:\s?(.*)$', line)
                if match:
                    parsed_lines.append(match.group(1))
                elif not parsed_lines:
                    continue
                else: 
                    # End of file
                    break
            
            if filepath not in file_contents or len(parsed_lines) > len(file_contents[filepath]):
                file_contents[filepath] = '\n'.join(parsed_lines)

for p, text in file_contents.items():
    if len(text.strip()) > 10:
        base = os.path.basename(p)
        dest = os.path.join(out_dir, base)
        try:
            with open(dest, 'w') as f:
                f.write(text)
            print(f'Recovered {base} ({len(text)} chars)')
        except Exception as e:
            print(f'Error writing {base}: {e}')
