#!/usr/bin/env python
import sys
import fileinput
import errno


if __name__ == "__main__":
    f = fileinput.input()

    all_lines = []
    error_lines = []
    module_line = None

    for line in f:
        line = line.strip()
        all_lines.append(line)

        if line[:12] == '************':
            module_line = line
            
        if line[:2] == 'E:':
            if module_line:
                error_lines.append(module_line)
                module_line = None

            error_lines.append(line)

    f.close()

    if len(error_lines):
        for line in error_lines:
            print(line)
        print("Aborting the build due the errors.")
        sys.exit(1)
    else:
        for line in all_lines:
            print(line)
        sys.exit(0)
