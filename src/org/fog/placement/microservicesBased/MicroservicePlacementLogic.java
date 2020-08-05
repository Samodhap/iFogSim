package org.fog.placement.microservicesBased;

import org.fog.application.Application;
import org.fog.entities.FogDevice;
import org.fog.entities.microservicesBased.PlacementRequest;

import java.util.List;
import java.util.Map;

/**
 * Created by Samodha Pallewatta
 */
public interface MicroservicePlacementLogic {
    Map<Integer, Map<String, Integer>> run(List<FogDevice> fogDevices, Map<String, Application> applicationInfo, Map<Integer, Map<String, Double>> resourceAvailability, List<PlacementRequest> pr);
    void postProcessing();
}
