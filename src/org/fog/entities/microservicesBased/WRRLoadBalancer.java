package org.fog.entities.microservicesBased;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Samodha Pallewatta
 * Weighted Round Robin LoadBalancer
 */
public class WRRLoadBalancer implements LoadBalancer {
    protected Map<String, Integer> loadBalancerPosition = new HashMap();


    public int getDeviceId(String microservice, ServiceDiscoveryInfo serviceDiscoveryInfo) {
        if (loadBalancerPosition.containsKey(microservice)) {
            int pos = loadBalancerPosition.get(microservice);
            if (pos + 1 > serviceDiscoveryInfo.getServiceDiscoveryInfo().get(microservice).size() - 1)
                pos = 0;
            else
                pos = pos + 1;
            loadBalancerPosition.put(microservice, pos);
            return serviceDiscoveryInfo.getServiceDiscoveryInfo().get(microservice).get(pos);
        } else {
            loadBalancerPosition.put(microservice, 0);
            return serviceDiscoveryInfo.getServiceDiscoveryInfo().get(microservice).get(0);
        }
    }
}
