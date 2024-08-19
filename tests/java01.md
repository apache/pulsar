## 文档注释

javadoc -d 文件夹名 -xx -yy Demo3.java

写在类之前，可以显示当前文档的相关信息

```java
/**
*@author XXX
*@version 1.0
*/
```

<img src="C:\Users\微光\AppData\Roaming\Typora\typora-user-images\image-20240713192123775.png" alt="image-20240713192123775" style="zoom: 50%;" />

## 相对路径、绝对路径

相对路径：当前目录开始定位形成的一个路径

..\表示回到上一级

..\ ..\hello.java两个表示回到上两级

绝对路径：从根目录出发形成的路径

d:\abd\test.....\hello.java

## DOS命令

dir	查看当前文件夹的文件

dir d:\xxx	查看当前具体文件夹里的文件

help cd 	查看当前指令的作用

cd /D c:	切换到其他盘

cd 相对路径或者绝对路径	切换到其他具体目录下

cd ..	切换到上一级

cd \	切换到根目录

tree	查看当前指定目录的所有子目录，目录树

cls	清屏

exit	退出DOS

md	创建目录

rd	删除目录

copy 拷贝文件

del 删除文件

echo 输入内容到文件

type	输入空文件

move	剪切

## 数据类型

#### 引用数据类型：数组、类、接口

#### 基本数据类型：

byte 字节，一个字节8bit

int 4byte

short 2byte

long 8byte，500L，后面接L

float 4byte，浮点数=符号位+指数位+尾数位，尾数部分可能丢失，造成精度损失(小数都是近似值)，后面接f或者F

double float 8byte，

浮点数的计数方式有两种：十进制计数，一定要有小数点，5.1,5.10F，.512F，.512==0.512；

科学计数法：512E-2==512*10的-2次，521E2==52100，double / float和int计算还是float，特别重要：判断两个数是否相等，应该是两个数差值的绝对值小于10的-6，因为有可能存在误差

char 单个字符，char c1 = 'a';	char c2 = '\t';	char c3 = '飞';	char c4 = '85'；char类型可以加减，对应数值

A-Z是65-90

a-z是97-122



## Java API文档

API是Java提供的基本编程接口，由Java自带的类还有相关的方法，中文在线文档：https：//www.matools.com；包里有接口、类、异常![image-20240718203857619](C:\Users\微光\AppData\Roaming\Typora\typora-user-images\image-20240718203857619.png)









