#! /usr/bin/env python3
# -*- coding=utf-8 -*-

from random import shuffle
from random import randint

# 定义每个单词的最大出现次数和总单词最小数
frequency = [20, 6000000]
anwerName="answer-1024.txt"
fileName="test-1024.txt"
words = [
    "cat",
    "pig",
    "dog",
    "rabbit",
    "mouse",
    "elephant",
    "panda",
    "tiger",
    "duck",
    "fish",
    "bird",
    "monkey",
    "chicken",
    # "lion",
    # "sheep",
    # "horse",
    # "giraffe",
    # "goat",
    # "wolf",
    # "goose",
    # "cow",
    # "blue",
    # "red",
    # "white",
    # "yellow",
    # "green",
    # "black",
    # "head",
    # "hair",
    # "eye",
    # "nose",
    # "face",
    # "neck",
    # "arm",
    "leg",
    "foot",
    "mouth",
    "hand",
    "finger",
    "toe",
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
    "nine",
    "ten",
]

words = [i+j+k+l for i in words for j in words for k in words for l in words]

# 随机这些单词出现的次数
dataset = {i: randint(0, frequency[0]) for i in words}

print("Words:",len(dataset))

# 当单词总数不够的时候放大一波
total = sum(i for i in dataset.values())
while total < frequency[1]:
    for i in dataset:
        dataset[i] = (int)(dataset[i] * 1.2)
    total = sum(i for i in dataset.values())
print("counts",total)

# 按照 频次降序 + 字典序 排序
words.sort(key=lambda x: (-dataset[x], x))
print("sorted")

# 写入正确答案
with open(anwerName, "w") as f:
    for i in words:
        if dataset[i] != 0:
            f.write(i + "\t" + str(dataset[i]) + "\n")
print("correct answer done")

# 写入测试集
with open(fileName, "w") as f:
    data = []
    for i, j in dataset.items():
        data.extend([i] * j)
    shuffle(data)
    for i,word in enumerate(data):
        if i%100000 == 1:
            f.write("\n")
        f.write(word)
        f.write(" ")
print("test generated")