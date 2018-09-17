### *List*

##### add 和 addAll 的区别

1. List.add() 方法，也是往List 中增加list，但是，它增加的是一个List 实例。如果，往容器中增加的那个List 实例从数据库中查到的结果有5条，不过，如果使用了List.add(list1);程序只会输出一条记录。原因就是上面说的。List.add() 加List 实例，它会把这个看一个实例，而不是把那个看成一个容器。**List.add() 的含义就是：你往这个List 中添加对象，它就把自己当初一个对象，你往这个List中添加容器，它就把自己当成一个容器。**

2. **List.addAll()方法，就是规定了，自己的这个List 就是容器，往里面增加的List 实例，增加到里面后，都会被看成对象。**

   > For Example：
   >
   > List.add(list1),List.add(list2);List.add(list3)，List.size 它的大小是3。List.add(list1),List.add(list2);List.add(list3)，List.size 它的大小就是所有list 实例化后的总数和总的记录数。

