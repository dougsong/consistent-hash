package com.songzheng.consistenthash;

import java.util.*;

/**
 * @author zhenran
 */
public class ConsistentHashingWithoutVirtualNode {

    /**
     * 集群地址列表
     */
    private static String[] groups = {
            "192.168.0.0:111", "192.168.0.1:111", "192.168.0.2:111",
            "192.168.0.3:111", "192.168.0.4:111"
    };

    /**
     * 用于保存Hash环上的节点
     */
    private static SortedMap<Integer, String> sortedMap = new TreeMap<>();

    /**
     * 初始化，将所有的服务器加入Hash环中
     */
    static {
        // 使用红黑树实现，插入效率较差，查找效率极高
        for (String group : groups) {
            int hash = HashUtil.getHash(group);
            System.out.println("[" + group + "] launched @ " + hash);
            sortedMap.put(hash, group);
        }
    }

    /**
     * 计算对应的请求落到哪一个节点
     * @param key
     * @return
     */
    private static String getServer(String key) {
        int hash = HashUtil.getHash(key);
        // 只取出所有大于该hash值的group，不必遍历整个tree
        SortedMap<Integer, String> subMap = sortedMap.tailMap(hash);
        if (subMap.isEmpty()) {
            // hash值在最尾部，映射到第一个group
            return sortedMap.get(sortedMap.firstKey());
        }
        return sortedMap.get(subMap.firstKey());
    }

    /**
     * 生成随机数测试
     * @param args
     */
    public static void main(String[] args) {
        Map<String, Integer> resMap = new HashMap<>();

        for (int i = 0; i < 100000; i++) {
            int widgetId = new Random().nextInt(10000);
            String server = getServer(Integer.toString(widgetId));
            if (resMap.containsKey(server)) {
                resMap.put(server, resMap.get(server) + 1);
            } else {
                resMap.put(server, 1);
            }
        }

        resMap.forEach((key, value) -> System.out.println("group " + key + ": " + value + "(" + value / 1000.0D + "%)"));
    }
}
