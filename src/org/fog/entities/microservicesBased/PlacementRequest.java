package org.fog.entities.microservicesBased;

import java.util.HashMap;
import java.util.Map;

public class PlacementRequest {
    private String applicationId;
    private Map<String,Integer> placedMicroservicesMap; // microservice name to placed device id
    private int placementRequestId; //sensor Id
    private int gatewayDeviceId; //device generating the request

    public PlacementRequest(String applicationId,int placementRequestId,int gatewayDeviceId,Map<String,Integer> placedMicroservicesMap){
        this.applicationId = applicationId;
        this.placementRequestId = placementRequestId;
        this.gatewayDeviceId = gatewayDeviceId;
        this.placedMicroservicesMap = placedMicroservicesMap;
    }

    public PlacementRequest(String applicationId,int placementRequestId,int gatewayDeviceId){
        this.applicationId = applicationId;
        this.placementRequestId = placementRequestId;
        this.gatewayDeviceId = gatewayDeviceId;
        this.placedMicroservicesMap = new HashMap<>();
    }

    public String getApplicationId(){
        return applicationId;
    }

    public int getPlacementRequestId() {
        return placementRequestId;
    }

    public int getGatewayDeviceId() {
        return gatewayDeviceId;
    }

    public Map<String, Integer> getPlacedMicroservicesMap() {
        return placedMicroservicesMap;
    }
}
