package org.fog.placement.microservicesBased;

import org.apache.commons.math3.util.Pair;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.fog.application.AppLoop;
import org.fog.application.Application;
import org.fog.entities.FogDevice;
import org.fog.entities.Sensor;
import org.fog.entities.microservicesBased.ClusteredFogDevice;
import org.fog.entities.microservicesBased.ControllerComponent;
import org.fog.entities.microservicesBased.LoadBalancer;
import org.fog.entities.microservicesBased.WRRLoadBalancer;
import org.fog.utils.*;

import java.util.*;

/**
 * Created by Samodha Pallewatta on 7/31/2020.
 */
public class ControllerM extends SimEntity {

    private List<FogDevice> fogDevices;
    private List<Sensor> sensors;
    private Map<String, Application> applications = new HashMap<>();
    private PlacementLogicFactory placementLogicFactory = new PlacementLogicFactory();

    /**
     * @param name
     * @param fogDevices
     * @param sensors
     * @param applications
     * @param clusterLevelIdentifier - Names of devices of this level starts with this id
     * @param clusterLatency
     */
    public ControllerM(String name, List<FogDevice> fogDevices, List<Sensor> sensors, List<Application> applications, List<String> clusterLevelIdentifier, Double clusterLatency, int placementLogic) {
        super(name);
        this.fogDevices = fogDevices;
        this.sensors = sensors;
        for (Application app : applications) {
            this.applications.put(app.getAppId(), app);
        }
        connectWithLatencies();
        for (String id : clusterLevelIdentifier)
            createClusterConnections(id, fogDevices, clusterLatency);
        printClusterConnections();

        initializeControllers(placementLogic);
        generateRoutingTable();
        setClientModulesToDeply();
    }

    private void printClusterConnections() {
        StringBuilder clusterString = new StringBuilder();
        clusterString.append("Cluster formation : ");
        // <ParentNode,ClusterNodes> Assuming than clusters are formed among nodes with same parent
        HashMap<String, List<ClusteredFogDevice>> clusters = new HashMap<>();
        for (FogDevice f : fogDevices) {
            ClusteredFogDevice cDevice = (ClusteredFogDevice) f;
            if (cDevice.isInCluster()) {
                FogDevice parent = getDevice(cDevice.getParentId());
                if (clusters.containsKey(parent.getName()))
                    clusters.get(parent.getName()).add(cDevice);
                else
                    clusters.put(parent.getName(), new ArrayList<>(Arrays.asList(cDevice)));
            }
        }
        for (String parent : clusters.keySet()) {
            List<ClusteredFogDevice> clusterNodes = clusters.get(parent);
            clusterString.append("Parent node : " + parent + " -> cluster Nodes : ");
            for (ClusteredFogDevice device : clusterNodes) {
                int count = device.getClusterNodeIds().size();
                clusterString.append(device.getName() + ", ");
                for (Integer deviceId : device.getClusterNodeIds()) {
                    if (!clusterNodes.contains(getDevice(deviceId))) {
                        Logger.error("Cluster formation Error", "Error : " + getDevice(deviceId).getName() + " is added as a cluster node of " + device.getName());
                    }
                }
                if (count + 1 != clusterNodes.size())
                    Logger.error("Cluster formation Error", "Error : number of cluster nodes does not match");
            }

            clusterString.append("\n");
        }
        System.out.println(clusterString);
    }


    private void setClientModulesToDeply() {
        for (Sensor s : sensors) {
            int gatewayDevice = s.getGatewayDeviceId();
            ClusteredFogDevice f = (ClusteredFogDevice) CloudSim.getEntity(gatewayDevice);
            Application app = s.getApp();
            f.setClientModulesToBeDeplyed(app, "client");
            // no need to update resource availability as client device is not considered for placement by policy.
        }
    }

    private void initializeControllers(int placementLogic) {
        for (FogDevice device : fogDevices) {
            LoadBalancer loadBalancer = new WRRLoadBalancer();
            ClusteredFogDevice cdevice = (ClusteredFogDevice) device;

            //responsible for placement decision making
            if (cdevice.getDevicType().equals(ClusteredFogDevice.FON) || cdevice.getDevicType().equals(ClusteredFogDevice.CLOUD)) {
                List<FogDevice> monitoredDevices = getDevicesForFON(cdevice);
                MicroservicePlacementLogic microservicePlacementLogic = placementLogicFactory.getPlacementLogic(placementLogic, cdevice.getId());
                cdevice.initializeController(loadBalancer, microservicePlacementLogic, getResourceInfo(monitoredDevices), applications, monitoredDevices);
            } else if (cdevice.getDevicType().equals(ClusteredFogDevice.FCN) || cdevice.getDevicType().equals(ClusteredFogDevice.CLIENT)) {
                cdevice.initializeController(loadBalancer);
            }
        }
    }

    private List<FogDevice> getDevicesForFON(FogDevice f) {
        List<FogDevice> fogDevcies = new ArrayList<>();
        fogDevcies.add(f);
        List<FogDevice> connected = new ArrayList<>();
        connected.add(f);
        boolean changed = true;
        while (changed) {
            changed = false;
            List<FogDevice> rootNodes = new ArrayList<>();
            for (FogDevice d : connected)
                rootNodes.add(d);
            for (FogDevice rootD : rootNodes) {
                for (int child : rootD.getChildrenIds()) {
                    FogDevice device = getDevice(child);
                    connected.add(device);
                    if (!fogDevcies.contains(device)) {
                        fogDevcies.add(device);
                        changed = true;
                    }
                }
                for (int cluster : ((ClusteredFogDevice) rootD).getClusterNodeIds()) {
                    FogDevice device = getDevice(cluster);
                    connected.add(device);
                    if (!fogDevcies.contains(device)) {
                        fogDevcies.add(device);
                        changed = true;
                    }
                }
                connected.remove(rootD);

            }
        }
        return fogDevcies;
    }


    private FogDevice getDevice(int id) {
        for (FogDevice f : fogDevices) {
            if (f.getId() == id)
                return f;
        }
        return null;
    }

    private void generateRoutingTable() {
        // <source device id>  ->  <dest device id,next device to route to>
        Map<Integer, Map<Integer, Integer>> routing = new HashMap<>();
        Map<String, Map<String, String>> routingString = new HashMap<>();
        int size = fogDevices.size();

        int[][] routingMatrix = new int[size][size];
        double[][] distanceMatrix = new double[size][size];
        for (int row = 0; row < size; row++) {
            for (int column = 0; column < size; column++) {
                routingMatrix[row][column] = -1;
                distanceMatrix[row][column] = -1;
            }
        }

        boolean change = true;
        boolean firstIteration = true;
        while (change || firstIteration) {
            change = false;
            for (int row = 0; row < size; row++) {
                for (int column = 0; column < size; column++) {
                    double dist = distanceMatrix[row][column];
                    FogDevice rFog = fogDevices.get(row);
                    FogDevice cFog = fogDevices.get(column);
                    if (firstIteration && dist < 0) {
                        if (row == column) {
                            dist = 0;
                        } else {
                            dist = directlyConnectedDist(rFog, cFog);
                        }
                        if (dist >= 0) {
                            change = true;
                            distanceMatrix[row][column] = dist;
                            distanceMatrix[column][row] = dist;

                            // directly connected
                            routingMatrix[row][column] = cFog.getId();
                            routingMatrix[column][row] = rFog.getId();
                        }
                    }
                    if (dist < 0) {
                        Pair<Double, Integer> result = indirectDist(row, column, size, distanceMatrix);
                        dist = result.getFirst();
                        int mid = result.getSecond();
                        if (dist >= 0) {
                            change = true;
                            distanceMatrix[row][column] = dist;
                            routingMatrix[row][column] = routingMatrix[row][mid];
                        }
                    }
                    if (dist > 0) {
                        Pair<Double, Integer> result = indirectDist(row, column, size, distanceMatrix);
                        double distNew = result.getFirst();
                        int mid = result.getSecond();
                        if (distNew < dist) {
                            change = true;
                            distanceMatrix[row][column] = distNew;
                            routingMatrix[row][column] = routingMatrix[row][mid];
                        }
                    }
                }
            }
            firstIteration = false;
        }

        for (int row = 0; row < size; row++) {
            for (int column = 0; column < size; column++) {
                int sourceId = fogDevices.get(row).getId();
                int destId = fogDevices.get(column).getId();
                if (routing.containsKey(sourceId)) {
                    routing.get(sourceId).put(destId, routingMatrix[row][column]);
                    routingString.get(fogDevices.get(row).getName()).put(fogDevices.get(column).getName(), getDevice(routingMatrix[row][column]).getName());
                } else {
                    Map<Integer, Integer> route = new HashMap<>();
                    route.put(destId, routingMatrix[row][column]);
                    routing.put(sourceId, route);

                    Map<String, String> routeS = new HashMap<>();
                    routeS.put(fogDevices.get(column).getName(), getDevice(routingMatrix[row][column]).getName());
                    routingString.put(fogDevices.get(row).getName(), routeS);
                }
            }
        }

        for (FogDevice f : fogDevices) {
            ((ClusteredFogDevice) f).addRoutingTable(routing.get(f.getId()));
        }

        System.out.println("Routing Table : ");
        for (String deviceName : routingString.keySet()) {
            System.out.println(deviceName + " : " + routingString.get(deviceName).toString());
        }
        System.out.println("\n");
    }

    private static Pair<Double, Integer> indirectDist(int row, int dest, int size, double[][] distanceMatrix) {
        double minDistFromDirectConn = distanceMatrix[row][dest];
        int midPoint = -1;
        for (int column = 0; column < size; column++) {
            if (distanceMatrix[row][column] >= 0 && distanceMatrix[column][dest] >= 0) {
                double totalDist = distanceMatrix[row][column] + distanceMatrix[column][dest];
                if (minDistFromDirectConn >= 0 && totalDist < minDistFromDirectConn) {
                    minDistFromDirectConn = totalDist;
                    midPoint = column;
                } else if (minDistFromDirectConn < 0) {
                    minDistFromDirectConn = totalDist;
                    midPoint = column;
                }
            }
        }
        return new Pair<>(minDistFromDirectConn, midPoint);
    }

    private static double directlyConnectedDist(FogDevice rFog, FogDevice cFog) {
        int parent = rFog.getParentId();
        List<Integer> children = rFog.getChildrenIds();
        List<Integer> cluster = ((ClusteredFogDevice) rFog).getClusterNodeIds();
        if (cFog.getId() == parent) {
            return rFog.getUplinkLatency();
        } else if (children != null && children.contains(cFog.getId())) {
            return rFog.getChildToLatencyMap().get(cFog.getId());
        } else if (cluster != null && cluster.contains(cFog.getId())) {
            return ((ClusteredFogDevice) rFog).getClusterNodeToLatencyMap().get(cFog.getId());
        }
        return -1;
    }

    public void startEntity() {
        send(getId(), Config.RESOURCE_MANAGE_INTERVAL, FogEvents.CONTROLLER_RESOURCE_MANAGE);

        send(getId(), Config.MAX_SIMULATION_TIME, FogEvents.STOP_SIMULATION);
    }

    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case FogEvents.CONTROLLER_RESOURCE_MANAGE:
                manageResources();
                break;
            case FogEvents.STOP_SIMULATION:
                CloudSim.stopSimulation();
                printTimeDetails();
                printPowerDetails();
                printCostDetails();
                printNetworkUsageDetails();
                System.exit(0);
                break;
        }

    }

    @Override
    public void shutdownEntity() {
    }

    protected void manageResources() {
        send(getId(), Config.RESOURCE_MANAGE_INTERVAL, FogEvents.CONTROLLER_RESOURCE_MANAGE);
    }

    private void printNetworkUsageDetails() {
        System.out.println("Total network usage = " + NetworkUsageMonitor.getNetworkUsage() / Config.MAX_SIMULATION_TIME);
    }

    private FogDevice getCloud() {
        for (FogDevice dev : fogDevices)
            if (dev.getName().equals("cloud"))
                return dev;
        return null;
    }

    private void printCostDetails() {
        System.out.println("Cost of execution in cloud = " + getCloud().getTotalCost());
    }

    private void printPowerDetails() {
        for (FogDevice fogDevice : fogDevices) {
            System.out.println(fogDevice.getName() + " : Energy Consumed = " + fogDevice.getEnergyConsumption());
        }
    }

    private String getStringForLoopId(int loopId) {
        for (String appId : applications.keySet()) {
            Application app = applications.get(appId);
            for (AppLoop loop : app.getLoops()) {
                if (loop.getLoopId() == loopId)
                    return loop.getModules().toString();
            }
        }
        return null;
    }

    private void printTimeDetails() {
        System.out.println("=========================================");
        System.out.println("============== RESULTS ==================");
        System.out.println("=========================================");
        System.out.println("EXECUTION TIME : " + (Calendar.getInstance().getTimeInMillis() - TimeKeeper.getInstance().getSimulationStartTime()));
        System.out.println("=========================================");
        System.out.println("APPLICATION LOOP DELAYS");
        System.out.println("=========================================");
        for (Integer loopId : TimeKeeper.getInstance().getLoopIdToTupleIds().keySet()) {
			/*double average = 0, count = 0;
			for(int tupleId : TimeKeeper.getInstance().getLoopIdToTupleIds().get(loopId)){
				Double startTime = 	TimeKeeper.getInstance().getEmitTimes().get(tupleId);
				Double endTime = 	TimeKeeper.getInstance().getEndTimes().get(tupleId);
				if(startTime == null || endTime == null)
					break;
				average += endTime-startTime;
				count += 1;
			}
			System.out.println(getStringForLoopId(loopId) + " ---> "+(average/count));*/
            System.out.println(getStringForLoopId(loopId) + " ---> " + TimeKeeper.getInstance().getLoopIdToCurrentAverage().get(loopId));
        }
        System.out.println("=========================================");
        System.out.println("TUPLE CPU EXECUTION DELAY");
        System.out.println("=========================================");

        for (String tupleType : TimeKeeper.getInstance().getTupleTypeToAverageCpuTime().keySet()) {
            System.out.println(tupleType + " ---> " + TimeKeeper.getInstance().getTupleTypeToAverageCpuTime().get(tupleType));
        }

        System.out.println("=========================================");
    }

    private void connectWithLatencies() {
        for (FogDevice fogDevice : fogDevices) {
            if (fogDevice.getParentId() >= 0) {
                FogDevice parent = (FogDevice) CloudSim.getEntity(fogDevice.getParentId());
                if (parent == null)
                    continue;
                double latency = fogDevice.getUplinkLatency();
                parent.getChildToLatencyMap().put(fogDevice.getId(), latency);
                parent.getChildrenIds().add(fogDevice.getId());
            }
        }
    }

    private Map<Integer, Map<String, Double>> getResourceInfo(List<FogDevice> fogDevices) {
        Map<Integer, Map<String, Double>> resources = new HashMap<>();
        for (FogDevice device : fogDevices) {
            Map<String, Double> perDevice = new HashMap<>();
            perDevice.put(ControllerComponent.CPU, (double) device.getHost().getTotalMips());
            perDevice.put(ControllerComponent.RAM, (double) device.getHost().getRam());
            perDevice.put(ControllerComponent.STORAGE, (double) device.getHost().getStorage());
            resources.put(device.getId(), perDevice);
        }
        return resources;
    }

    private static void createClusterConnections(String levelIdentifier, List<FogDevice> fogDevices, Double clusterLatency) {
        Map<Integer, List<FogDevice>> fogDevicesByParent = new HashMap<>();
        for (FogDevice fogDevice : fogDevices) {
            if (fogDevice.getName().startsWith(levelIdentifier)) {
                if (fogDevicesByParent.containsKey(fogDevice.getParentId())) {
                    fogDevicesByParent.get(fogDevice.getParentId()).add(fogDevice);
                } else {
                    List<FogDevice> sameParentList = new ArrayList<>();
                    sameParentList.add(fogDevice);
                    fogDevicesByParent.put(fogDevice.getParentId(), sameParentList);
                }
            }
        }

        for (int parentId : fogDevicesByParent.keySet()) {
            List<Integer> clusterNodeIds = new ArrayList<>();
            for (FogDevice fogdevice : fogDevicesByParent.get(parentId)) {
                clusterNodeIds.add(fogdevice.getId());
            }
            for (FogDevice fogDevice : fogDevicesByParent.get(parentId)) {
                List<Integer> clusterNodeIdsTemp = new ArrayList<>(clusterNodeIds);
                clusterNodeIds.remove((Object) fogDevice.getId());
                ((ClusteredFogDevice) fogDevice).setClusterNodeIds(clusterNodeIds);
                Map<Integer, Double> latencyMap = new HashMap<>();
                for (int id : clusterNodeIds) {
                    latencyMap.put(id, clusterLatency);
                }
                ((ClusteredFogDevice) fogDevice).setClusterNodeToLatency(latencyMap);
                ((ClusteredFogDevice) fogDevice).setInCluster(true);
                clusterNodeIds = clusterNodeIdsTemp;

            }
        }
    }

}
