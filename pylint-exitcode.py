#!/usr/bin/env python
import sys
import fileinput
import errno


if __name__ == "__main__":
    f = fileinput.input()
    errors_count = 0

    for line in f:
        line = line.strip()
        if line[:2] == 'E:':
            errors_count += 1
        print(line)

    f.close()

    if errors_count > 0:
        sys.exit(1)
    else:
        sys.exit(0)
