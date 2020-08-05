package org.fog.placement.microservicesBased;

import org.cloudbus.cloudsim.core.CloudSim;
import org.fog.application.AppEdge;
import org.fog.application.AppModule;
import org.fog.application.Application;
import org.fog.entities.FogDevice;
import org.fog.entities.Tuple;
import org.fog.entities.microservicesBased.ClusteredFogDevice;
import org.fog.entities.microservicesBased.ControllerComponent;
import org.fog.entities.microservicesBased.PlacementRequest;
import org.fog.placement.Controller;
import org.fog.utils.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Samodha Pallewatta on 7/28/2020.
 */
public class EdgewardMicroservicePlacementLogic implements MicroservicePlacementLogic {

    /**
     * Fog network related details
     */
    //todo change just to function arguements
    List<FogDevice> fogDevices; //fog devices considered by FON for placements of requests
    List<PlacementRequest> placementRequests; // requests to be processed
    protected Map<Integer, Map<String, Double>> resourceAvailability = new HashMap<>();
    private Map<String, Application> applicationInfo = new HashMap<>();


    int fonID;
    List<String> microservicesIdentifierList = new ArrayList<>();
    // <microservicesId,prID>
    Map<String, Integer> mForPrMap = new HashMap<>();

    protected Map<Integer, Double> currentCpuLoad;
    // microserviceName ,uniqueMicroserviceID list
    protected Map<Integer, Map<String, List<String>>> currentModuleMap = new HashMap<>();
    protected Map<Integer, Map<String, Double>> currentModuleLoadMap = new HashMap<>();
    protected Map<Integer, Map<String, Integer>> currentModuleInstanceNum = new HashMap<>();

    public EdgewardMicroservicePlacementLogic(int fonID) {
        setFONId(fonID);
    }

    public void setFONId(int id) {
        fonID = id;
    }

    public int getFonID() {
        return fonID;
    }

    @Override
    public Map<Integer, Map<String, Integer>> run(List<FogDevice> fogDevices, Map<String, Application> applicationInfo, Map<Integer, Map<String, Double>> resourceAvailability, List<PlacementRequest> pr) {
        this.fogDevices = fogDevices;
        this.placementRequests = pr;
        this.resourceAvailability = resourceAvailability;
        this.applicationInfo = applicationInfo;

        setCurrentCpuLoad(new HashMap<Integer, Double>());
        setCurrentModuleMap(new HashMap<>());
        for (FogDevice dev : fogDevices) {
            getCurrentCpuLoad().put(dev.getId(), 0.0);
            getCurrentModuleMap().put(dev.getId(), new HashMap<>());
            currentModuleLoadMap.put(dev.getId(), new HashMap<String, Double>());
            currentModuleInstanceNum.put(dev.getId(), new HashMap<String, Integer>());
        }


        mapModules();
        Map<Integer, Map<String, Integer>> placement = generatePlacementMap();
        postProcessing();
        return placement;
    }

    private Map<Integer, Map<String, Integer>> generatePlacementMap() {
        Map<Integer, Map<String, Integer>> placement = new HashMap<>();
        for (int deviceID : currentModuleMap.keySet()) {
            for (String microserviceName : currentModuleMap.get(deviceID).keySet()) {
                for (String mID : currentModuleMap.get(deviceID).get(microserviceName)) {
                    if (placement.containsKey(mForPrMap.get(mID))) {
                        placement.get(mForPrMap.get(mID)).put(microserviceName, deviceID);
                    } else {
                        Map<String, Integer> map = new HashMap<>();
                        map.put(microserviceName, deviceID);
                        placement.put(mForPrMap.get(mID), map);
                    }
                }
            }
        }
        return placement;
    }

    public void setPlacementRequest(PlacementRequest placementRequest) {
        placementRequests.add(placementRequest);
    }

    @Override
    public void postProcessing() {
        //remove processed placement requests
        this.fogDevices = new ArrayList<>();
        this.placementRequests = new ArrayList<>();
        this.applicationInfo = new HashMap<>();

        //update resources
        //todo update other resources as well
        for(int deviceID:resourceAvailability.keySet()){
            double remainingCpu = resourceAvailability.get(deviceID).get(ControllerComponent.CPU)-getCurrentCpuLoad().get(deviceID);
            resourceAvailability.get(deviceID).put(ControllerComponent.CPU,remainingCpu);
        }

        this.resourceAvailability = new HashMap<>();
        setCurrentCpuLoad(new HashMap<Integer, Double>());
        setCurrentModuleMap(new HashMap<>());
        currentModuleMap = new HashMap<>();
        currentModuleInstanceNum = new HashMap<>();
    }

    public void addFogDeviceToList(FogDevice fogDevice) {
        fogDevices.add(fogDevice);
    }

    private void generateMicroservicesList() {
        for (PlacementRequest placementRequest : placementRequests) {
            Application application = applicationInfo.get(placementRequest.getApplicationId());
            List<AppModule> microservicesPerApp = application.getModules();
            for (AppModule microservice : microservicesPerApp) {
                // todo make this consistant
                String microserviceID = placementRequest.getPlacementRequestId() + "_" + application.getAppId() + "_" + microservice.getName();
                if (!placementRequest.getPlacedMicroservicesMap().keySet().contains(microservice.getName())) {
                    microservicesIdentifierList.add(microserviceID);
                    mForPrMap.put(microserviceID, placementRequest.getPlacementRequestId());
                }
            }
        }
    }

    public void mapModules() {

        //generate microservices list to place if needed
        generateMicroservicesList();

        List<List<Integer>> leafToRootPaths = getLeafToRootPaths();

        for (List<Integer> path : leafToRootPaths) {
            placeModulesInPath(path);
        }

        int i = 0;
        for (int device : currentModuleMap.keySet()) {
            for (String module : currentModuleMap.get(device).keySet()) {
                i += currentModuleMap.get(device).get(module).size();
            }
        }

    }

    protected List<List<Integer>> getLeafToRootPaths() {
        return getPaths(fonID);
    }

    protected List<List<Integer>> getPaths(final int fogDeviceId) {
        FogDevice device = (FogDevice) CloudSim.getEntity(fogDeviceId);
        if (device.getChildrenIds().size() == 0) {
            final List<Integer> path = (new ArrayList<Integer>() {{
                add(fogDeviceId);
            }});
            List<List<Integer>> paths = (new ArrayList<List<Integer>>() {{
                add(path);
            }});
            return paths;
        }
        List<List<Integer>> paths = new ArrayList<List<Integer>>();
        for (int childId : device.getChildrenIds()) {
            List<List<Integer>> childPaths = getPaths(childId);
            for (List<Integer> childPath : childPaths)
                childPath.add(fogDeviceId);
            paths.addAll(childPaths);
        }
        return paths;
    }

    private void placeModulesInPath(List<Integer> path) {
        if (path.size() == 0) return;

        //get placement requests generated by the leaf device od the path
        List<PlacementRequest> placementRequests = getPlacementRequest(path.get(0));
        for (PlacementRequest placementRequest : placementRequests) {

            //microserviceName
            List<String> placedModules = new ArrayList<String>();

            // client module is already placed
            for (String placed : placementRequest.getPlacedMicroservicesMap().keySet()) {
                placedModules.add(placed);
            }

            Application app = applicationInfo.get(placementRequest.getApplicationId());

            for (Integer deviceId : path) {
                FogDevice device = getFogDeviceById(deviceId);
                if(((ClusteredFogDevice)device).getDevicType().equals(ClusteredFogDevice.CLIENT))
                    continue;
                /*
                 * Getting the list of modules ready to be placed on current device on path
                 * in microsrviceName
                 */
                List<String> modulesToPlace = getModulesToPlace(placedModules, app);

                while (modulesToPlace.size() > 0) { // Loop runs until all modules in modulesToPlace are deployed in the path
                    String moduleName = modulesToPlace.get(0);

                    //IF MODULE IS ALREADY PLACED UPSTREAM, THEN UPDATE THE EXISTING MODULE
                    int upsteamDeviceId = isPlacedUpstream(moduleName, path);
                    if (upsteamDeviceId > 0) {
                        if (upsteamDeviceId == deviceId) {
                            placedModules.add(moduleName);

                            //module name
                            modulesToPlace = getModulesToPlace(placedModules, app);

                            if (getModule(moduleName, app).getMips() + getCurrentCpuLoad().get(deviceId) > resourceAvailability.get(device.getId()).get("cpu")) {
                                Logger.debug("ModulePlacementEdgeward", "Need to shift module " + moduleName + " upstream from device " + device.getName());
                                String uniqueID = placementRequest.getPlacementRequestId() + "_" + app.getAppId() + "_" + moduleName;
                                List<String> _placedOperators = shiftModuleNorth(moduleName, getModule(moduleName, app).getMips(), deviceId, modulesToPlace, app, uniqueID);
                                for (String placedOperator : _placedOperators) {
                                    if (!placedModules.contains(placedOperator))
                                        placedModules.add(placedOperator);
                                }
                            } else {
                                String uniqueID = placementRequest.getPlacementRequestId() + "_" + app.getAppId() + "_" + moduleName;
                                placedModules.add(moduleName);
                                getCurrentCpuLoad().put(deviceId, getCurrentCpuLoad().get(deviceId) + getModule(moduleName, app).getMips());
                                //module instance
                                currentModuleInstanceNum.get(deviceId).put(moduleName, currentModuleInstanceNum.get(deviceId).get(moduleName) + 1);
                                (currentModuleMap.get(deviceId).get(moduleName)).add(uniqueID);
                                Logger.debug("ModulePlacementEdgeward", "AppModule " + moduleName + " can be created on device " + device.getName());
                            }
                        }
                    } else {
                        if (getModule(moduleName, app).getMips() + getCurrentCpuLoad().get(deviceId) > device.getHost().getTotalMips()) {
                            Logger.debug("ModulePlacementEdgeward", "Placement of operator " + moduleName + "NOT POSSIBLE on device " + device.getName());
                        } else {
                            Logger.debug("ModulePlacementEdgeward", "Placement of operator " + moduleName + " on device " + device.getName() + " successful.");
                            getCurrentCpuLoad().put(deviceId, getModule(moduleName, app).getMips() + getCurrentCpuLoad().get(deviceId));
                            System.out.println("Placement of operator " + moduleName + " on device " + device.getName() + " successful.");

                            String uniqueID = placementRequest.getPlacementRequestId() + "_" + app.getAppId() + "_" + moduleName;
                            if (currentModuleMap.get(deviceId).containsKey(moduleName))
                                currentModuleMap.get(deviceId).get(moduleName).add(uniqueID);
                            else {
                                List<String> l = new ArrayList<>();
                                l.add(uniqueID);
                                currentModuleMap.get(deviceId).put(moduleName, l);
                            }

                            placedModules.add(moduleName);
                            modulesToPlace = getModulesToPlace(placedModules, app);


                            //currentModuleLoad
                            currentModuleLoadMap.get(device.getId()).put(moduleName, getModule(moduleName, app).getMips());

                            //currentModuleInstance
                            currentModuleInstanceNum.get(deviceId).put(moduleName, 1);
                        }
                    }


                    modulesToPlace.remove(moduleName);
                }

            }
        }

    }

    /**
     * find if theres a placement request generated from the deviceId.
     *
     * @param deviceId
     * @return
     */
    private List<PlacementRequest> getPlacementRequest(int deviceId) {
        List<PlacementRequest> placementRequests = new ArrayList<>();
        for (PlacementRequest placementRequest : this.placementRequests) {
            if (placementRequest.getGatewayDeviceId() == deviceId)
                placementRequests.add(placementRequest);
        }
        return placementRequests;
    }

    private FogDevice getFogDeviceById(Integer deviceId) {
        for (FogDevice device : fogDevices) {
            if (device.getId() == deviceId)
                return device;
        }
        return null;
    }

    /**
     * Get the list of modules that are ready to be placed
     *
     * @param placedModules Modules that have already been placed in current path
     * @return list of modules ready to be placed
     */
    private List<String> getModulesToPlace(List<String> placedModules, Application app) {
        List<String> modulesToPlace_1 = new ArrayList<String>();
        List<String> modulesToPlace = new ArrayList<String>();
        for (AppModule module : app.getModules()) {
            if (!placedModules.contains(module.getName()))
                modulesToPlace_1.add(module.getName());
        }
        /*
         * Filtering based on whether modules (to be placed) lower in physical topology are already placed
         */
        for (String moduleName : modulesToPlace_1) {
            boolean toBePlaced = true;

            for (AppEdge edge : app.getEdges()) {
                //CHECK IF OUTGOING DOWN EDGES ARE PLACED
                if (edge.getSource().equals(moduleName) && edge.getDirection() == Tuple.DOWN && !placedModules.contains(edge.getDestination()))
                    toBePlaced = false;
                //CHECK IF INCOMING UP EDGES ARE PLACED
                if (edge.getDestination().equals(moduleName) && edge.getDirection() == Tuple.UP && !placedModules.contains(edge.getSource()))
                    toBePlaced = false;
            }
            if (toBePlaced)
                modulesToPlace.add(moduleName);
        }

        return modulesToPlace;
    }

    private int isPlacedUpstream(String operatorName, List<Integer> path) {
        for (int deviceId : path) {
            if (currentModuleMap.containsKey(deviceId)) {
                if (currentModuleMap.get(deviceId).keySet().contains(operatorName))
                    return deviceId;
            }
        }
        return -1;
    }

    private AppModule getModule(String moduleName, Application app) {
        for (AppModule appModule : app.getModules()) {
            if (appModule.getName().equals(moduleName))
                return appModule;
        }
        return null;
    }

    /**
     * Shifts a module moduleName from device deviceId northwards. This involves other modules that depend on it to be shifted north as well.
     *
     * @param moduleName
     * @param cpuLoad    cpuLoad of the module
     * @param deviceId
     */
    private List<String> shiftModuleNorth(String moduleName, double cpuLoad, Integer deviceId, List<String> operatorsToPlace, Application app, String microserviceUniqueID) {
        System.out.println(CloudSim.getEntityName(deviceId) + " is shifting " + moduleName + " north.");
        List<String> modulesToShift = findModulesToShift(moduleName, deviceId, app);

        Map<String, Integer> moduleToNumInstances = new HashMap<String, Integer>(); // Map of number of instances of modules that need to be shifted
        double totalCpuLoad = 0;
        Map<String, Double> loadMap = new HashMap<String, Double>();
        //name and uniqueIDs per name
        Map<String, List<String>> microservicesToMove = new HashMap<>();
        for (String module : modulesToShift) {
            loadMap.put(module, currentModuleLoadMap.get(deviceId).get(module));
            moduleToNumInstances.put(module, currentModuleInstanceNum.get(deviceId).get(module) + 1);
            totalCpuLoad += currentModuleLoadMap.get(deviceId).get(module);
            for (String name : currentModuleMap.get(deviceId).keySet()) {
                if (name.equals(module)) {
                    if (name.equals(moduleName)) {
                        List<String> l = currentModuleMap.get(deviceId).get(name);
                        l.add(microserviceUniqueID);
                        microservicesToMove.put(module, l);
                    } else
                        microservicesToMove.put(module, currentModuleMap.get(deviceId).get(name));
                    currentModuleMap.get(deviceId).remove(name);
                }
            }
            currentModuleInstanceNum.get(deviceId).remove(module);
        }

        getCurrentCpuLoad().put(deviceId, getCurrentCpuLoad().get(deviceId) - totalCpuLoad); // change info of current CPU load on device
        loadMap.put(moduleName, loadMap.get(moduleName) + cpuLoad);
        totalCpuLoad += cpuLoad;

        int id = getParentDevice(deviceId);
        while (true) { // Loop iterates over all devices in path upstream from current device. Tries to place modules (to be shifted northwards) on each of them.
            if (id == -1) {
                // Loop has reached the apex fog device in hierarchy, and still could not place modules.
                Logger.debug("ModulePlacementEdgeward", "Could not place modules " + modulesToShift + " northwards.");
                break;
            }
            FogDevice fogDevice = getFogDeviceById(id);
            if (getCurrentCpuLoad().get(id) + totalCpuLoad > resourceAvailability.get(fonID).get(ControllerComponent.CPU)) {
                // Device cannot take up CPU load of incoming modules. Keep searching for device further north.
                List<String> _modulesToShift = findModulesToShift(modulesToShift, id, app);    // All modules in _modulesToShift are currently placed on device id
                double cpuLoadShifted = 0;        // the total CPU load shifted from device id to its parent
                for (String module : _modulesToShift) {
                    if (!modulesToShift.contains(module)) {
                        // Add information of all newly added modules (to be shifted)
                        moduleToNumInstances.put(module, currentModuleInstanceNum.get(id).get(module) + moduleToNumInstances.get(module));
                        loadMap.put(module, currentModuleLoadMap.get(id).get(module));
                        cpuLoadShifted += currentModuleLoadMap.get(id).get(module);
                        totalCpuLoad += currentModuleLoadMap.get(id).get(module);
                        // Removing information of all modules (to be shifted north) in device with ID id
                        currentModuleLoadMap.get(id).remove(module);
                        getCurrentModuleMap().get(id).remove(module);
                        currentModuleInstanceNum.get(id).remove(module);
                    }
                }
                getCurrentCpuLoad().put(id, getCurrentCpuLoad().get(id) - cpuLoadShifted); // CPU load on device id gets reduced due to modules shifting northwards

                modulesToShift = _modulesToShift;
                id = getParentDevice(id); // iterating to parent device
            } else {
                // Device (@ id) can accommodate modules. Placing them here.
                double totalLoad = 0;
                for (String module : loadMap.keySet()) {
                    totalLoad += loadMap.get(module);
                    currentModuleLoadMap.get(id).put(module, loadMap.get(module));
                    getCurrentModuleMap().get(id).put(module, microservicesToMove.get(module));
                    String module_ = module;
                    int initialNumInstances = 0;
                    if (currentModuleInstanceNum.get(id).containsKey(module_))
                        initialNumInstances = currentModuleInstanceNum.get(id).get(module_);
                    int finalNumInstances = initialNumInstances + moduleToNumInstances.get(module_);
                    currentModuleInstanceNum.get(id).put(module_, finalNumInstances);
                }
                getCurrentCpuLoad().put(id, totalLoad);
                operatorsToPlace.removeAll(loadMap.keySet());
                List<String> placedOperators = new ArrayList<String>();
                for (String op : loadMap.keySet()) placedOperators.add(op);
                return placedOperators;
            }
        }
        return new ArrayList<String>();
    }

    /**
     * Get all modules that need to be shifted northwards along with <b>module</b>.
     * Typically, these other modules are those that are hosted on device with ID <b>deviceId</b> and lie upstream of <b>module</b> in application model.
     *
     * @param module   the module that needs to be shifted northwards
     * @param deviceId the fog device ID that it is currently on
     * @return list of all modules that need to be shifted north along with <b>module</b>
     */
    private List<String> findModulesToShift(String module, Integer deviceId, Application app) {
        List<String> modules = new ArrayList<String>();
        modules.add(module);
        return findModulesToShift(modules, deviceId, app);
		/*List<String> upstreamModules = new ArrayList<String>();
		upstreamModules.add(module);
		boolean changed = true;
		while(changed){ // Keep loop running as long as new information is added.
			changed = false;
			for(AppEdge edge : getApplication().getEdges()){

				 * If there is an application edge UP from the module to be shifted to another module in the same device

				if(upstreamModules.contains(edge.getSource()) && edge.getDirection()==Tuple.UP &&
						getCurrentModuleMap().get(deviceId).contains(edge.getDestination())
						&& !upstreamModules.contains(edge.getDestination())){
					upstreamModules.add(edge.getDestination());
					changed = true;
				}
			}
		}
		return upstreamModules;	*/
    }

    /**
     * Get all modules that need to be shifted northwards along with <b>modules</b>.
     * Typically, these other modules are those that are hosted on device with ID <b>deviceId</b> and lie upstream of modules in <b>modules</b> in application model.
     *
     * @param deviceId the fog device ID that it is currently on
     * @return list of all modules that need to be shifted north along with <b>modules</b>
     */
    private List<String> findModulesToShift(List<String> modules, Integer deviceId, Application app) {
        List<String> upstreamModules = new ArrayList<String>();
        upstreamModules.addAll(modules);
        boolean changed = true;
        while (changed) { // Keep loop running as long as new information is added.
            changed = false;
            /*
             * If there is an application edge UP from the module to be shifted to another module in the same device
             */
            for (AppEdge edge : app.getEdges()) {
                if (upstreamModules.contains(edge.getSource()) && edge.getDirection() == Tuple.UP &&
                        getCurrentModuleMap().get(deviceId).keySet().contains(edge.getDestination())
                        && !upstreamModules.contains(edge.getDestination())) {
                    upstreamModules.add(edge.getDestination());
                    changed = true;
                }
            }
        }
        return upstreamModules;
    }

    public Map<Integer, Map<String, List<String>>> getCurrentModuleMap() {
        return currentModuleMap;
    }

    public void setCurrentModuleMap(Map<Integer, Map<String, List<String>>> currentModuleMap) {
        this.currentModuleMap = currentModuleMap;
    }


    public Map<Integer, Double> getCurrentCpuLoad() {
        return currentCpuLoad;
    }

    public void setCurrentCpuLoad(Map<Integer, Double> currentCpuLoad) {
        this.currentCpuLoad = currentCpuLoad;
    }

    private int getParentDevice(Integer deviceId) {
        for (FogDevice fogDevice : fogDevices) {
            if (fogDevice.getId() == deviceId)
                return fogDevice.getParentId();
        }
        return -1;
    }

}
