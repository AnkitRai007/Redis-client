import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class RedisClient {

    public static final String COLON = ":";

    public static void main(String args[]) throws IOException {
        if( args.length < 6 ) {
            System.out.println("Invalid number of arguments passed, required 6 but received: "+args.length);
            System.out.println("[Usage] java -cp lib/*:. RedisClient <RedisClusterNodes> <RedisUserName> <RedisPwd> <RedisIsSSL[true/false] <KeysFilePath> <Operation[add/get/delete/get-pattern/delete-pattern]> ");
            System.out.println("[Example] java -cp lib/*:. RedisClient 192.168.13.120:9001,192.168.13.120:9002,192.168.13.121:9001,192.168.13.121:9002,192.168.13.122:9001,192.168.13.122:9002 redis redis true add /tmp/redis-add.txt");
            System.exit(-9);
        }
        String redisHosts = args[0].trim();
        String redisUsername = args[1].trim();
        String redisPwd = args[2].trim();
        boolean redisSSLEnabled = Boolean.parseBoolean(args[3].trim());
        String operation = (args[4].trim()); // add, get, delete, get-pattern, delete-pattern
        String filenameOrPattern = args[5].trim();


        Set<KeyDetails> set = new HashSet<>();
        if(!operation.equalsIgnoreCase("pattern")) {
            File f = new File(filenameOrPattern);
            if (!f.exists()) {
                System.err.println("File does not exists. File:" + filenameOrPattern);
                System.exit(-9);
            }


            BufferedReader br = new BufferedReader(new FileReader(filenameOrPattern));
            String s;
            while ((s = br.readLine()) != null) {
                KeyDetails keyDetails = new KeyDetails();
                String[] lineSplit = s.split(",");
                if (lineSplit.length < 2) {
                    System.err.println("Ignoring line because it does not have correct format." + s);
                    continue;
                }
                String key = lineSplit[0];
                String hash = lineSplit[1];
                String value = "";
                if (lineSplit.length >= 3) {
                    value = lineSplit[2];
                }

                long expiry = 0;

                try {
                    if (lineSplit.length >= 4) {
                        expiry = Long.parseLong(lineSplit[4]);
                    }
                } catch (NumberFormatException e) {
                    // ignore
                }
                keyDetails.setKey(key);
                keyDetails.setHash(hash);
                keyDetails.setValue(value);
                keyDetails.setExpiry(expiry);
                set.add(keyDetails);
            }
            if(set.isEmpty()) {
                System.err.println("Invalid keys exists in the file or file is empty.");
                System.exit(-9);
            }
        }


        RedisClient client = new RedisClient();
        RedisAdvancedClusterCommands<String, String> redisClient = client.getRedisClient(redisHosts, redisUsername, redisPwd, redisSSLEnabled);

        if(redisClient == null) {
            System.err.println("Could not connect to redis. Check the input parameters");
            System.exit(-9);
        }

        switch (operation){
            case "add": {client.pushKeys(set,redisClient); break;}
            case "get" : {client.getKeys(set, redisClient);break;}
            case "get-pattern" : {client.getKeysByPatter(filenameOrPattern, redisClient);break;}
            case "delete-pattern" : {client.deleteKeysByPatter(filenameOrPattern, redisClient);break;}
            case "delete": {client.deleteKeys(set, redisClient);break;}
            default:
                System.out.println("Invalid option selected, should only add/get/delete");
                break;
        }
    }

    public static class KeyDetails {
        private String key;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getHash() {
            return hash;
        }

        public void setHash(String hash) {
            this.hash = hash;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public long getExpiry() {
            return expiry;
        }

        public void setExpiry(long expiry) {
            this.expiry = expiry;
        }

        private String hash;
        private String value;
        private long expiry;



        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            KeyDetails that = (KeyDetails) o;
            return Objects.equals(key, that.key) && Objects.equals(hash, that.hash) && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, hash, value);
        }
    }
    public void pushKeys(Set<KeyDetails> keyDetails, RedisAdvancedClusterCommands redisClient) {

        AtomicInteger insertCount = new AtomicInteger();
        keyDetails.forEach(keyDetail -> {
            insertCount.addAndGet((redisClient.hset(keyDetail.getKey(), keyDetail.getHash(), keyDetail.getValue()) == true) ? 1 : 0);
            if(keyDetail.getExpiry() > 0)
                redisClient.expire(keyDetail.getKey(), keyDetail.getExpiry());
        });

        System.out.println("Number of key inserted:"+insertCount+", keys in file:"+keyDetails.size());
    }

    public void deleteKeys(Set<KeyDetails> keyDetails, RedisAdvancedClusterCommands redisClient) {
        int delCount = 0;
        for (KeyDetails keyDetail : keyDetails) {
            delCount += redisClient.del(keyDetail.getKey());
        }
        /*redisClient.hdel(keyDetails.stream().map(k -> k.getKey()).collect(Collectors.toList()));*/
        System.out.println("Number of keys deleted:"+delCount+" , keys in file:"+keyDetails.size());
    }

    public void getKeys(Set<KeyDetails> keyDetails, RedisAdvancedClusterCommands redisClient) {

        keyDetails.forEach(k -> {
            Object value = redisClient.hget(k.getKey(), k.getHash());
            System.out.println("Key:"+k.getKey()+", Hash:"+k.getHash()+", Value:"+((value == null) ? "NA":value.toString()));
        });
    }
    public void getKeysByPatter(String pattern, RedisAdvancedClusterCommands redisClient) {
        System.out.println("Pattern:"+pattern);
        List list = redisClient.keys(pattern);
        list.forEach(k -> System.out.println("Key:"+k));
    }
    public void deleteKeysByPatter(String pattern, RedisAdvancedClusterCommands redisClient) {
        System.out.println("Pattern:"+pattern);
        List list = redisClient.keys(pattern);
        System.out.println("Deleting the keys:"+redisClient.del(list)+", pattern:"+list.size());

    }

    public RedisAdvancedClusterCommands<String, String> getRedisClient(String redisHosts, String username, String password, boolean isSSL) {

        if (redisHosts == null || redisHosts.trim().isEmpty()) {
            return null;
        }

        List<String> redisHostDetails = Arrays.asList(redisHosts.split(","));
        if (redisHostDetails.isEmpty()) {
            return null;
        }
        return getClusterModeRedisClient(redisHostDetails, username, password, isSSL);

    }

    private RedisAdvancedClusterCommands<String, String> getClusterModeRedisClient(List<String> redisHostDetails, String username, String plainPassword, boolean sslEnabled) {

        try {

            List<RedisURI> redisNodes = redisHostDetails.stream()
                    .map(hostPort -> {
                        try {
                            String[] hostAndPort = hostPort.split(COLON);
                            RedisURI node = RedisURI.Builder
                                    .redis(hostAndPort[0])
                                    .withSsl(sslEnabled)
                                    .withPort(Integer.parseInt(hostAndPort[1]))
                                    .build();
                            if (username != null && plainPassword != null) {
                                node.setUsername(username);
                                node.setPassword(plainPassword.toCharArray());
                            }
                            return node;
                        } catch (Exception e) {
                            e.printStackTrace();
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            if (redisNodes.isEmpty()) {
                System.err.println("Redis node details are empty. redisHostDetails:"+redisHostDetails);
                return null;
            }


            RedisClusterClient clusterClient = RedisClusterClient.create(redisNodes);
            clusterClient.setOptions(ClusterClientOptions.builder()
                    .autoReconnect(true)
                    .validateClusterNodeMembership(true)
                    .maxRedirects(5)
                    .cancelCommandsOnReconnectFailure(true)
                    .pingBeforeActivateConnection(true)
                    .topologyRefreshOptions(ClusterTopologyRefreshOptions.builder()
                            .enablePeriodicRefresh()
                            .build())
                    .build());
            return clusterClient.connect().sync();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}