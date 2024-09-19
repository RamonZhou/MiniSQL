from random import *

N = 10000
for i in range(N):
    print(f"insert into account values({1200000 + i:d}, \"name{i:05d}\", {randint(1, 1000) + random():.2f});")