### UDF

> 1. **自定义UDF的时候要继承org.apache.hadoop.hive.ql.exec.UDAF**；
> 2. **需要实现evaluate函数**；
> 3. **evaluate函数需要支持重载**。

```java
///Example
public class Avg extends UDAF {
    public Integer evaluate(Integer a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public Double evaluate(Double a, Double b) {
        if (a == 0.0 || b == 0.0) {
            return 0.0;
        }
        return a + b;
    }

    public Integer evaluate(Integer... a) {
        int total = 0;
        for (int i = 0; i < a.length; i++) {
            if (a[i] != null) {
                total += a[i];
            }
        }
        return total;
    }
}
```



### UDAF

>1. 必须继承
>
>   **org.apache.hadoop.hive.ql.exec.UDAF(函数类继承)**
>
>   **org.apache.hadoop.hive.ql.exec.UDAFEvaluator(内部类Evaluator实现UDAFEvaluator接口)**
>
>2. **Evaluator需要实现 init、iterate、terminatePartial、merge、terminate这几个函数**。
>
>   init()：类似于构造函数，用于UDAF的初始化
>
>   iterate()：接收传入的参数，并进行内部的轮转。其返回类型为boolean
>
>   terminatePartial()：无参数，其为iterate函数轮转结束后，返回乱转数据，iterate和terminatePartial类似于hadoop的Combiner（iterate--mapper;terminatePartial--reducer）
>
>   merge()：接收terminatePartial的返回结果，进行数据merge操作，其返回类型为boolean
>
>   terminate()：返回最终的聚集函数结果

```java
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class AvgUDAF extends UDAF {
    public static class AvgState {
        private long mcount;
        private double msum;
    }

    public static class AvgEvaluator implements UDAFEvaluator {
        AvgState state;
        public AvgEvaluator() {
            super();
            state = new AvgState();
            init();
        }

        public void init() {
            state.mcount = 0;
            state.msum = 0;
        }

        public boolean iterable(double o) {
            if (o != 0) {
                state.msum += o;
                state.mcount++;
            }
            return true;
        }

        public AvgState terminatePartial() {
            return state.mcount == 0 ? state : null;
        }

        public boolean merge(AvgState o) {
            if (o != null) {
                state.mcount += o.mcount;
                state.msum += o.msum;
            }
            return true;
        }

        public Double terminate() {
            return state.mcount == 0 ? null : Double.valueOf(state.msum / state.mcount);
        }
    }
}

```

