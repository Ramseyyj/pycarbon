def FileInputStream(filename):
  try:
    print(1)
    f = open(filename)
    for line in f:
      for byte in line:
        yield byte
  except StopIteration as e:
    f.close()
    return


path = "/Users/xubo/Desktop/xubo/git/pycarbon/pycarbon/tests/data/image/carbondatalogo.jpg"
path2 = "/Users/xubo/Desktop/xubo/git/pycarbon/pycarbon/tests/data/image/carbondatalogo2.jpg"

import matplotlib.pyplot as plt  # plt 用于显示图片
import matplotlib.image as mpimg  # mpimg 用于读取图片
import numpy as np

lena = mpimg.imread(path)  # 读取和代码处于同一目录下的 lena.png
# 此时 lena 就已经是一个 np.array 了，可以对它进行任意处理
lena.shape  # (512, 512, 3)
plt.imshow(lena)  # 显示图片
plt.axis('off')  # 不显示坐标轴
plt.savefig(path2)
plt.show()

from skimage import io

img = io.imread(path)
io.imshow(img)
# print(path)
# data = FileInputStream(path)
#
# print(1)
# f = open(path)
# for line in f:
#   for byte in line:
#     print(len(byte))
# f.close()

# path = "/Users/xubo/Desktop/xubo/git/pycarbon/pycarbon/tests/data/image/carbondatalogo.jpg"
# print(path)
# data = FileInputStream(path)
# a = 2 + 1
# print(a)
# with open(path) as file_object:
#   sum = 0
#   for line in file_object:
#     sum = sum + len(line)
#     for byte in line:
#       # yield byte
#       print(byte)
#   print(sum)
