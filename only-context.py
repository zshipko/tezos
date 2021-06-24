import sys

input = open(sys.argv[1], 'rb')
lines = input.readlines()
input.close()

for line in lines:
    if b'[context]' in line:
        index = line.index(b'[context]') + 10
        sys.stdout.buffer.write(line[index:])
