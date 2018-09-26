```java
/** *
 * 给定 nums = [2, 7, 11, 15], target = 9
 * 因为 nums[0] + nums[1] = 2 + 7 = 9
 * 所以返回 [0, 1]
 */
public int[] twoSum(int[] nums, int target) {
    Map<Integer, Integer> tmp = new HashMap<>();
    for (int i = 0; i < nums.length; i++) {
        int flag = target - nums[i];
        if (tmp.containsKey(flag)) {
            return new int[] {tmp.get(flag), i};
        }
        tmp.put(nums[i], i);
    }
    throw new IllegalArgumentException("No two sum solution");
}
```

