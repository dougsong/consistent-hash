package com.songzheng.consistenthash;

import org.springframework.util.StringUtils;

import java.util.*;

/**
 * @author zhenran
 */
public class ConsistentHashingWithVirtualNode {

    /**
     * 集群地址列表
     */
    private static String[] groups = {
            "192.168.0.0:111", "192.168.0.1:111", "192.168.0.2:111",
            "192.168.0.3:111", "192.168.0.4:111"
    };

    /**
     * 真实集群列表
     */
    private static List<String> realGroups = new LinkedList<>();

    /**
     * 虚拟节点映射关系
     */
    private static SortedMap<Integer, String> virtualNodes = new TreeMap<>();

    private static final int VIRTUAL_NODE_NUM = 10000;

    /**
     * 初始化，将所有的服务器加入Hash环中
     */
    static {
        // 先添加真实节点列表
        realGroups.addAll(Arrays.asList(groups));

        // 将虚拟节点映射到hash环
        for (String realGroup : realGroups) {
            for (int i = 0; i < VIRTUAL_NODE_NUM; i++) {
                String virtualNodeName = getVirtualNodeName(realGroup, i);
                int hash = HashUtil.getHash(virtualNodeName);
                virtualNodes.put(hash, virtualNodeName);
                System.out.println("[" + virtualNodeName + "] launched @ " + hash);
            }
        }
    }

    private synchronized static void refreshHashCircle() {
        virtualNodes.clear();
        for (String realGroup : realGroups) {
            for (int i = 0; i < VIRTUAL_NODE_NUM; i++) {
                String virtualNodeName = getVirtualNodeName(realGroup, i);
                int hash = HashUtil.getHash(virtualNodeName);
                virtualNodes.put(hash, virtualNodeName);
//                System.out.println("[" + virtualNodeName + "] launched @ " + hash);
            }
        }
    }

    private synchronized static void addGroup(String identifier) {
        realGroups.add(identifier);
        refreshHashCircle();
    }

    private synchronized static void removeGroup(String identifier) {
        realGroups.removeIf(s -> s.equals(identifier));
        refreshHashCircle();
    }

    private static String getVirtualNodeName(String realGroup, int i) {
        return String.format("%s&&VN-%d", realGroup, i);
    }

    private static String getRealGroupFromVirtualNode(String virtualNodeName) {
        if (StringUtils.isEmpty(virtualNodeName)) {
            return null;
        }
        return virtualNodeName.split("&&")[0];
    }

    /**
     * 计算对应的请求落到哪一个节点
     * @param key
     * @return
     */
    private static String getServer(String key) {
        int hash = HashUtil.getHash(key);
        // 只取出所有大于该hash值的group，不必遍历整个tree
        SortedMap<Integer, String> subMap = virtualNodes.tailMap(hash);
        if (subMap.isEmpty()) {
            // hash值在最尾部，映射到第一个group
            return getRealGroupFromVirtualNode(virtualNodes.get(virtualNodes.firstKey()));
        }
        return getRealGroupFromVirtualNode(virtualNodes.get(subMap.firstKey()));
    }

    /**
     * 生成随机数测试
     * @param args
     */
    public static void main(String[] args) throws InterruptedException {
        Map<String, Integer> resMap = new HashMap<>();

        for (int i = 0; i < 10000000; i++) {
            int widgetId = new Random().nextInt(10000);
            String server = getServer(Integer.toString(widgetId));
            if (resMap.containsKey(server)) {
                resMap.put(server, resMap.get(server) + 1);
            } else {
                resMap.put(server, 1);
            }
        }

        new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            removeGroup("192.168.0.0:111");
            System.out.println("removeGroup success");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            addGroup("192.168.0.0:10086");
            System.out.println("addGroup success");
        }).start();

        resMap.forEach((key, value) -> System.out.println("group " + key + ": " + value + "(" + value / 100000.0D + "%)"));
    }
}
