#!/usr/bin/env python
import sys
import fileinput


def sanitize(in_String):
    if "]" in in_String[:-1]:
        print(in_String.replace("\n", "").strip())
        a = in_String.replace("'", "\"").replace("\n", "").replace("[", "{").replace("]", "}").replace("|", " OR ")
        a = a[:a.find("(") - 1]
        return a
    return in_String.replace("'", "\"").replace("\n", "").strip()

if __name__ == "__main__":
    firstSuite = True
    lastSuite = ""
    f = fileinput.input()
    for i in f:
        # print ':::%s' % i
        if len(i.strip()) == 0:
            continue
        if i[0] == "*":
            suiteName = i.split(" ")[2].replace("\n", "")
            if not firstSuite:
                print("##teamcity[testSuiteFinished name='%s' ]" % lastSuite)
            else:
                firstSuite = False
            print("##teamcity[testSuiteStarted name='%s' ]" % suiteName)
            lastSuite = suiteName
        else:
            parts = i.split(':')
            if len(parts) > 2:
                msgnum = sanitize(parts[0])
                linenum = sanitize(parts[1])
                if len(parts) >= 4:
                    msg = sanitize(parts[3])
                else:
                    msg = sanitize(parts[2])
                tcmsg = '(%s) %s' % (linenum, msg)
                print("##teamcity[testStarted name='%s' ]" % tcmsg)
                print("##teamcity[testFailed name='%s' message='%s' details='%s' ]" % (tcmsg, msg, msgnum))
                print("##teamcity[testFinished name='%s' ]" % tcmsg)
    print("##teamcity[testSuiteFinished name='%s' ]" % lastSuite)
    f.close()
