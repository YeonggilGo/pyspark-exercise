# sc: Spark Context in spark shell
lines = sc.textFile("/path")
bsd_lines = lines.filter(lambda line: "BSD" in line)
bsd_lines.count()

from __future__ import print_function

bsd_lines.foreach(lambda b_line: print(b_line))


def isBSD(line):
    return "BSD" in line


bsd_lines = lines.filter(isBSD)
bsd_lines.count()
bsd_lines.foreach(lambda b_lines: print(b_line))
